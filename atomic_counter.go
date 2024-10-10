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

type AtomicCounter struct {
	db           *Chotki
	rid          rdx.ID
	offset       uint64
	localValue   atomic.Int64
	tlv          atomic.Value
	rdt          atomic.Value
	loaded       atomic.Bool
	lock         sync.Mutex
	expiration   time.Time
	updatePeriod time.Duration
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

func (a *AtomicCounter) load() error {
	a.lock.Lock()
	defer a.lock.Unlock()
	now := time.Now()
	if a.loaded.Load() && now.Sub(a.expiration) < 0 {
		return nil
	}
	rdt, tlv, err := a.db.ObjectFieldTLV(a.rid.ToOff(a.offset))
	if err != nil {
		return err
	}
	a.rdt.Store(rdt)
	a.loaded.Store(true)
	a.tlv.Store(tlv)
	switch rdt {
	case rdx.ZCounter:
		a.localValue.Store(rdx.Znative(tlv))
	case rdx.Natural:
		a.localValue.Store(int64(rdx.Nnative(tlv)))
	default:
		return ErrNotCounter
	}
	a.expiration = now.Add(a.updatePeriod)
	return nil
}

// Loads (if needed) and increments counter
func (a *AtomicCounter) Increment(ctx context.Context, val int64) (int64, error) {
	err := a.load()
	if err != nil {
		return 0, err
	}
	if val == 2 {
		fmt.Println(1)
	}
	rdt := a.rdt.Load().(byte)
	a.localValue.Add(val)
	var dtlv []byte
	a.lock.Lock()
	tlv := a.tlv.Load().([]byte)
	switch rdt {
	case rdx.Natural:
		dtlv = rdx.Ndelta(tlv, uint64(a.localValue.Load()), a.db.Clock())
	case rdx.ZCounter:
		dtlv = rdx.Zdelta(tlv, a.localValue.Load(), a.db.Clock())
	default:
		return 0, ErrNotCounter
	}
	a.tlv.Store(dtlv)
	a.lock.Unlock()
	changes := make(protocol.Records, 0)
	changes = append(changes, protocol.Record('F', rdx.ZipUint64(uint64(a.offset))))
	changes = append(changes, protocol.Record(rdt, dtlv))
	a.db.CommitPacket(ctx, 'E', a.rid, changes)
	return a.localValue.Load(), nil
}
