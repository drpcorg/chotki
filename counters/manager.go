package counters

import (
	"context"
	"sync"
	"time"

	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
)

// AtomicCounterManager owns per-field AtomicCounters; a mutex serializes DB I/O while the hot
// path (Get/Increment) never takes it.
type AtomicCounterManager struct {
	mu     sync.Mutex
	period time.Duration
	states sync.Map // rdx.ID (rid.ToOff(offset)) -> *AtomicCounter
	db     host.Host
	log    utils.Logger
}

func NewAtomicCounterManager(db host.Host, period time.Duration, log utils.Logger) *AtomicCounterManager {
	return &AtomicCounterManager{db: db, period: period, log: log}
}

// Counter returns the counter for (rid, offset), creating it on first use; failed initial loads
// are retried each background cycle.
func (m *AtomicCounterManager) Counter(rid rdx.ID, offset uint64) *AtomicCounter {
	key := rid.ToOff(offset)
	if existing, ok := m.states.Load(key); ok {
		return existing.(*AtomicCounter)
	}
	c := newAtomicCounter(m.db, rid, offset)
	actual, loaded := m.states.LoadOrStore(key, c)
	if loaded {
		return actual.(*AtomicCounter) // lost the race; use the winner's counter
	}
	m.mu.Lock()
	if err := c.load(); err != nil && m.log != nil {
		m.log.Warn("counter initial load failed", "rid", rid.String(), "offset", offset, "err", err)
	}
	m.mu.Unlock()
	return c
}

// Cycle forces a flush + full reload of all counters; used by SyncCounters and tests.
func (m *AtomicCounterManager) Cycle(ctx context.Context) {
	m.cycle(ctx, true)
}

// cycle flushes dirty counters, then reloads all (force) or only touched/unloaded ones (background tick uses force=false).
func (m *AtomicCounterManager) cycle(ctx context.Context, force bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushAllLocked(ctx)
	// Swap runs unconditionally to clear the accessed flag even when skipping reload.
	m.states.Range(func(_, v any) bool {
		c := v.(*AtomicCounter)
		if c.accessed.Swap(false) || force || !c.loaded.Load() {
			if err := c.load(); err != nil && m.log != nil {
				m.log.Warn("counter load failed", "rid", c.rid.String(), "offset", c.offset, "err", err)
			}
		}
		return true
	})
}

type pendingMark struct {
	c        *AtomicCounter
	syncedTo int64
}

// maxFlushBatch caps edits per CommitBatch; a var so tests can shrink it.
var maxFlushBatch = 1024

// flushAllLocked commits changed counters in maxFlushBatch chunks; failed chunks are retried next
// cycle. Caller holds m.mu.
func (m *AtomicCounterManager) flushAllLocked(ctx context.Context) {
	var edits []host.Edit
	var marks []pendingMark
	m.states.Range(func(_, v any) bool {
		c := v.(*AtomicCounter)
		changed, rdt, op, syncedTo := c.pendingFlush()
		if !changed {
			return true
		}
		edits = append(edits, host.Edit{
			Ref: c.rid.ZeroOff(),
			Body: protocol.Records{
				protocol.Record('F', rdx.ZipUint64(c.offset)),
				protocol.Record(rdt, op),
			},
		})
		marks = append(marks, pendingMark{c, syncedTo})
		return true
	})
	// Each chunk is all-or-nothing; a failed chunk is retried next cycle.
	for start := 0; start < len(edits); start += maxFlushBatch {
		end := start + maxFlushBatch
		if end > len(edits) {
			end = len(edits)
		}
		if err := m.db.CommitBatch(ctx, edits[start:end]); err != nil {
			if m.log != nil {
				m.log.Warn("counter batch flush failed", "edits", end-start, "err", err)
			}
			continue // this chunk is retried next cycle
		}
		for _, mk := range marks[start:end] {
			mk.c.markSynced(mk.syncedTo)
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
	m.mu.Lock()
	defer m.mu.Unlock()
	// CommitBatch ignores cancellation internally, so Background is safe.
	m.flushAllLocked(context.Background())
}
