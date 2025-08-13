// Provides AtomicCounter - a high-performance atomic counter implementation
// for distributed systems with CRDT semantics.

package counters

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
)

var ErrNotCounter error = fmt.Errorf("not a counter")
var ErrCounterNotLoaded error = fmt.Errorf("counter not loaded")
var ErrDecrementN error = fmt.Errorf("decrementing natural counter")

type AtomicCounter struct {
	data         atomic.Value
	db           host.Host
	rid          rdx.ID
	offset       uint64
	lock         sync.RWMutex
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

// NewAtomicCounter creates a new atomic counter instance.
//
// The counter uses lazy loading with time-based caching. When updatePeriod > 0,
// data is cached to avoid expensive database reads, but may return stale values.
// When updatePeriod = 0, fresh data is always read from the database.
func NewAtomicCounter(db host.Host, rid rdx.ID, offset uint64, updatePeriod time.Duration) *AtomicCounter {
	return &AtomicCounter{
		db:           db,
		rid:          rid,
		offset:       offset,
		updatePeriod: updatePeriod,
	}
}

// load retrieves and caches counter data from the database.
//
// Uses double-checked locking: first checks cache without lock, then acquires
// write lock only if cache is expired. Loads TLV data from database and parses
// into internal structures (atomicNcounter for Natural, atomicZCounter for ZCounter).
// This method only affects how frequently we read synchronized data from other replicas.
// Local writes are always immediately visible regardless of cache state.
func (a *AtomicCounter) load() (any, error) {
	now := time.Now()
	if a.data.Load() != nil && now.Sub(a.expiration) < 0 {
		return a.data.Load(), nil
	}

	a.lock.RUnlock()
	a.lock.Lock()
	defer func() {
		a.lock.Unlock()
		a.lock.RLock()
	}()

	if a.data.Load() != nil && now.Sub(a.expiration) < 0 {
		return a.data.Load(), nil
	}

	rdt, tlv, err := a.db.ObjectFieldTLV(a.rid.ToOff(a.offset))
	if err != nil {
		return nil, err
	}
	var data any
	switch rdt {
	case rdx.ZCounter:
		total, mine, rev := rdx.Znative3(tlv, a.db.Source())
		part := zpart{total: total, revision: rev}
		c := atomicZCounter{
			theirs: total - mine,
			part:   atomic.Pointer[zpart]{},
		}
		c.part.Store(&part)
		data = &c
	case rdx.Natural:
		total, mine := rdx.Nnative2(tlv, a.db.Source())
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
	return data, nil
}

// Get retrieves the current value of the counter.
//
// Acquires read lock, loads data (cached or from DB), and returns the total value.
// For Natural counters returns sum of all replica contributions, for ZCounter returns current total.
func (a *AtomicCounter) Get(ctx context.Context) (int64, error) {
	a.lock.RLock()
	defer a.lock.RUnlock()
	data, err := a.load()
	if err != nil {
		return 0, err
	}
	switch c := data.(type) {
	case *atomicNcounter:
		return int64(c.total.Load()), nil
	case *atomicZCounter:
		return c.part.Load().total, nil
	default:
		return 0, ErrCounterNotLoaded
	}
}

// Increment atomically increments the counter by the specified value.
//
// Loads current data, performs atomic update using Go primitives (atomic.Uint64 for Natural,
// CompareAndSwap for ZCounter), generates TLV data, and commits to database with CRDT semantics.
// Natural counters only allow positive increments, ZCounter supports both positive and negative.
func (a *AtomicCounter) Increment(ctx context.Context, val int64) (int64, error) {
	a.lock.RLock()
	defer a.lock.RUnlock()
	data, err := a.load()
	if err != nil {
		return 0, err
	}
	var dtlv []byte
	var result int64
	var rdt byte
	switch c := data.(type) {
	case *atomicNcounter:
		if val < 0 {
			return 0, ErrDecrementN
		}
		nw := c.total.Add(uint64(val))
		dtlv = rdx.Ntlvt(nw-c.theirs, a.db.Source())
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
				dtlv = rdx.Ztlvt(nw.total-c.theirs, a.db.Source(), nw.revision)
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
