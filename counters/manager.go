package counters

import (
	"context"
	"errors"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
)

// negCacheTTL bounds how long a "field is not a counter" result is cached; a var
// so tests can shrink it.
var negCacheTTL = time.Second

// negCacheSize caps the number of negative entries (LRU eviction).
const negCacheSize = 1024

// AtomicCounterManager owns per-field AtomicCounters; a mutex serializes DB I/O while the hot
// path (Get/Increment) never takes it.
type AtomicCounterManager struct {
	mu     sync.Mutex
	period time.Duration
	states sync.Map // rdx.ID (rid.ToOff(offset)) -> *AtomicCounter
	db     host.Host
	log    utils.Logger
	// negCache rate-limits load retries for not-a-counter fields (ErrNotCounter),
	// which would otherwise re-hit the DB and log on every call/tick. Entries
	// expire so a field that becomes a counter self-heals (not a permanent latch).
	negCache *lru.LRU[rdx.ID, struct{}]
}

func NewAtomicCounterManager(db host.Host, period time.Duration, log utils.Logger) *AtomicCounterManager {
	return &AtomicCounterManager{
		db:       db,
		period:   period,
		log:      log,
		negCache: lru.NewLRU[rdx.ID, struct{}](negCacheSize, nil, negCacheTTL),
	}
}

// Counter returns the counter for (rid, offset), creating and loading it on first use; a load that
// failed (object not local yet) is retried on each call and by the background cycle.
func (m *AtomicCounterManager) Counter(rid rdx.ID, offset uint64) *AtomicCounter {
	key := rid.ToOff(offset)
	existing, ok := m.states.Load(key)
	if !ok {
		// lost races fall through to the winner's counter
		existing, _ = m.states.LoadOrStore(key, newAtomicCounter(m.db, rid, offset))
	}
	c := existing.(*AtomicCounter)
	m.ensureLoaded(c) // first load, or retry of a previously-failed one; no-op once loaded
	return c
}

// ensureLoaded retries the load for a cached counter whose object wasn't local at first touch;
// no-op once loaded. Serialized with flush/reload via m.mu; the hot path never reaches the lock.
func (m *AtomicCounterManager) ensureLoaded(c *AtomicCounter) {
	if c.loaded.Load() {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if c.loaded.Load() {
		return
	}
	m.loadLocked(c)
}

// loadLocked runs c.load() unless the field was recently found to be not a
// counter (negCache). On success it clears accessed; on a transient failure it
// leaves accessed set so the next cycle retries. Caller holds m.mu.
func (m *AtomicCounterManager) loadLocked(c *AtomicCounter) {
	key := c.rid.ToOff(c.offset)
	if _, bad := m.negCache.Get(key); bad {
		return // recently not-a-counter; skip the DB hit, expires soon
	}
	if err := c.load(); err != nil {
		if errors.Is(err, ErrNotCounter) {
			m.negCache.Add(key, struct{}{})
		}
		if m.log != nil {
			m.log.Warn("counter load failed", "rid", c.rid.String(), "offset", c.offset, "err", err)
		}
		return
	}
	c.accessed.Store(false) // only after a successful load
}

// Cycle forces a flush + full reload of all counters; used by SyncCounters and tests.
func (m *AtomicCounterManager) Cycle(ctx context.Context) {
	m.cycle(ctx, true)
}

// cycle flushes dirty counters, then reloads all (force) or only touched/unloaded ones (background tick uses force=false).
func (m *AtomicCounterManager) cycle(ctx context.Context, force bool) {
	// Lock order: bracket → m.mu, matching RMW flows (bracket → Counter →
	// m.mu). The reverse order deadlocks against them.
	m.db.StartSequentialWrite()
	defer m.db.EndSequentialWrite()

	m.mu.Lock()
	defer m.mu.Unlock()

	// snapshot states once, then drive both flush and reload from it. (No
	// eviction — states grows with the number of distinct counters touched.)
	snapshot := m.snapshotLocked()

	// Read-modify-write under one bracket: reload FIRST so baselines
	// (value, rev) are current, then flush deltas on top. A Z flush stamped
	// from a stale rev collides with a concurrent reset and Zmerge silently
	// drops one of the writes.
	for _, c := range snapshot {
		// accessed is read, not swapped: loadLocked clears it only on a
		// successful load, so a transient failure keeps the reload pending.
		if c.accessed.Load() || force || !c.loaded.Load() {
			m.loadLocked(c)
		}
	}
	m.flushLocked(ctx, snapshot)
}

// snapshotLocked collects the current counters into a slice. Caller holds m.mu.
func (m *AtomicCounterManager) snapshotLocked() []*AtomicCounter {
	snapshot := make([]*AtomicCounter, 0)
	m.states.Range(func(_, v any) bool {
		snapshot = append(snapshot, v.(*AtomicCounter))
		return true
	})
	return snapshot
}

// maxFlushBatch caps edits per CommitBatch; a var so tests can shrink it.
var maxFlushBatch = 1024

// flushLocked commits changed counters in maxFlushBatch chunks; a failed chunk
// is retried next cycle. Caller holds the sequential-write bracket and m.mu,
// in that order. The bracket spans all chunks (every op is read before the
// first commit) so a concurrent RMW writer of the same slot can't lose a write.
func (m *AtomicCounterManager) flushLocked(ctx context.Context, snapshot []*AtomicCounter) {
	var edits []host.Edit
	var commits []func()
	for _, c := range snapshot {
		changed, rdt, op, onCommit := c.pendingFlush()
		if !changed {
			continue
		}
		edits = append(edits, host.Edit{
			Ref: c.rid.ZeroOff(),
			Body: protocol.Records{
				protocol.Record('F', rdx.ZipUint64(c.offset)),
				protocol.Record(rdt, op),
			},
		})
		commits = append(commits, onCommit)
	}
	// Each chunk is all-or-nothing; a failed chunk is retried next cycle.
	for start := 0; start < len(edits); start += maxFlushBatch {
		end := min(start+maxFlushBatch, len(edits))
		if err := m.db.CommitBatch(ctx, edits[start:end]); err != nil {
			if m.log != nil {
				m.log.Warn("counter batch flush failed", "edits", end-start, "err", err)
			}
			continue // this chunk is retried next cycle
		}
		for _, onCommit := range commits[start:end] {
			onCommit()
		}
	}
}

// Run is the background goroutine; ticks cycle every period and flushes on shutdown.
func (m *AtomicCounterManager) Run(ctx context.Context) {
	if m.period <= 0 {
		<-ctx.Done()
		m.flushOnShutdown()
		return
	}
	t := time.NewTicker(m.period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.flushOnShutdown()
			return
		case <-t.C:
			m.cycle(ctx, false)
		}
	}
}

func (m *AtomicCounterManager) flushOnShutdown() {
	// lock order: bracket → m.mu (see cycle)
	m.db.StartSequentialWrite()
	defer m.db.EndSequentialWrite()
	m.mu.Lock()
	defer m.mu.Unlock()
	// CommitBatch ignores cancellation internally, so Background is safe.
	m.flushLocked(context.Background(), m.snapshotLocked())
}
