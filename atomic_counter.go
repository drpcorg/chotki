package chotki

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

var ErrNotCounter error = fmt.Errorf("not a counter")
var ErrCounterNotLoaded error = fmt.Errorf("counter not loaded")
var ErrDecrementN error = fmt.Errorf("decrementing natural counter")

type AtomicCounter struct {
	data         atomic.Value
	db           *Chotki
	wg           sync.WaitGroup
	rid          rdx.ID
	offset       uint64
	lock         sync.Mutex
	expiration   time.Time
	updatePeriod time.Duration
}

type atomicNcounter struct {
	theirs uint64
	total  atomic.Uint64
}

type zpart struct {
	total    int64
	revision int64
}

type atomicZCounter struct {
	theirs int64
	part   atomic.Pointer[zpart]
}

// creates counter that has two properties
//   - its atomic as long as you use single instance to do all increments, creating multiple instances will break this guarantee
//   - it can ease CPU load if updatePeiod > 0, in that case it will not read from db backend
//     current value of the counter
//
// Because we use LSM backend writes are cheap, reads are expensive. You can trade off up to date value of counter
// for less CPU cycles
func NewAtomicCounter(db *Chotki, rid rdx.ID, offset uint64, updatePeriod time.Duration) *AtomicCounter {
	return &AtomicCounter{
		db:           db,
		rid:          rid,
		offset:       offset,
		updatePeriod: updatePeriod,
	}
}

func (a *AtomicCounter) load() (any, error) {
	now := time.Now()
	if a.data.Load() != nil && now.Sub(a.expiration) < 0 {
		a.wg.Add(1)
		return a.data.Load(), nil
	}
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.data.Load() != nil && now.Sub(a.expiration) < 0 {
		a.wg.Add(1)
		return a.data.Load(), nil
	}
	a.wg.Wait()
	rdt, tlv, err := a.db.ObjectFieldTLV(a.rid.ToOff(a.offset))
	if err != nil {
		return nil, err
	}
	var data any
	switch rdt {
	case rdx.ZCounter:
		total, mine, rev := rdx.Znative3(tlv, a.db.clock.Src())
		part := zpart{total: total, revision: rev}
		c := atomicZCounter{
			theirs: total - mine,
			part:   atomic.Pointer[zpart]{},
		}
		c.part.Store(&part)
		data = &c
	case rdx.Natural:
		total, mine := rdx.Nnative2(tlv, a.db.clock.Src())
		c := atomicNcounter{
			theirs: total - mine,
			total:  atomic.Uint64{},
		}
		c.total.Add(total)
		data = &c
	default:
		return nil, ErrNotCounter
	}
	a.data.Store(data)
	a.expiration = now.Add(a.updatePeriod)
	a.wg.Add(1)
	return data, nil
}

func (a *AtomicCounter) Get(ctx context.Context) (int64, error) {
	data, err := a.load()
	if err != nil {
		return 0, err
	}
	defer a.wg.Done()
	switch c := data.(type) {
	case *atomicNcounter:
		return int64(c.total.Load()), nil
	case *atomicZCounter:
		return c.part.Load().total, nil
	default:
		return 0, ErrCounterNotLoaded
	}
}

// Loads (if needed) and increments counter
func (a *AtomicCounter) Increment(ctx context.Context, val int64) (int64, error) {
	data, err := a.load()
	if err != nil {
		return 0, err
	}
	defer a.wg.Done()
	var dtlv []byte
	var result int64
	var rdt byte
	switch c := data.(type) {
	case *atomicNcounter:
		if val < 0 {
			return 0, ErrDecrementN
		}
		nw := c.total.Add(uint64(val))
		dtlv = rdx.Ntlvt(nw-c.theirs, a.db.clock.Src())
		result = int64(nw)
		rdt = rdx.Natural
	case *atomicZCounter:
		for {
			current := c.part.Load()
			nw := zpart{
				total:    current.total + val,
				revision: current.revision + 1,
			}
			ok := c.part.CompareAndSwap(current, &nw)
			if ok {
				dtlv = rdx.Ztlvt(nw.total-c.theirs, a.db.clock.Src(), nw.revision)
				result = nw.total
				rdt = rdx.ZCounter
				break
			}
		}
	default:
		return 0, ErrCounterNotLoaded
	}
	changes := make(protocol.Records, 0)
	changes = append(changes, protocol.Record('F', rdx.ZipUint64(uint64(a.offset))))
	changes = append(changes, protocol.Record(rdt, dtlv))
	a.db.CommitPacket(ctx, 'E', a.rid.ZeroOff(), changes)
	return result, nil
}
