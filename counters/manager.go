package counters

import (
	"context"
	"sync"
	"time"

	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
)

// AtomicCounterManager owns the per-field AtomicCounters, a mutex that serializes all DB I/O
// (load/flush), and a background goroutine that periodically flushes local contributions and
// reloads others'. The hot path (Counter().Get/Increment) never takes the mutex, so the
// goroutine can never block it.
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

// Counter returns the counter for (rid, offset), creating it on first use. On creation it
// attempts the baseline load; on failure the counter stays unloaded (Get/Increment return
// ErrCounterNotLoaded) and the background tick retries the load every cycle until it succeeds.
func (m *AtomicCounterManager) Counter(rid rdx.ID, offset uint64) *AtomicCounter {
	key := rid.ToOff(offset)
	if existing, ok := m.states.Load(key); ok {
		return existing.(*AtomicCounter)
	}
	c := newAtomicCounter(m.db, rid, offset)
	actual, loaded := m.states.LoadOrStore(key, c)
	if loaded {
		return actual.(*AtomicCounter) // someone else won the race; use theirs
	}
	m.mu.Lock()
	if err := c.load(); err != nil && m.log != nil {
		m.log.Warn("counter initial load failed", "rid", rid.String(), "offset", offset, "err", err)
	}
	m.mu.Unlock()
	return c
}

// Cycle forces one flush + full reload pass over every counter under the mutex. Used by the
// explicit Chotki.SyncCounters entry point (and tests) when the caller wants everything synced.
func (m *AtomicCounterManager) Cycle(ctx context.Context) {
	m.cycle(ctx, true)
}

// cycle flushes every dirty counter, then reloads either every counter (force) or only the
// ones touched since the last cycle / still unloaded (the background tick uses force=false so
// idle loaded counters cost nothing beyond a cheap flush no-op).
func (m *AtomicCounterManager) cycle(ctx context.Context, force bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states.Range(func(_, v any) bool {
		c := v.(*AtomicCounter)
		if err := c.flush(ctx); err != nil && m.log != nil {
			m.log.Warn("counter flush failed", "rid", c.rid.String(), "offset", c.offset, "err", err)
		}
		// Reload when: a hot-path op touched it (refresh theirs), a force cycle was requested,
		// or it never loaded yet (retry the baseline until it succeeds — the Swap must run
		// unconditionally to clear the accessed flag).
		if c.accessed.Swap(false) || force || !c.loaded.Load() {
			if err := c.load(); err != nil && m.log != nil {
				m.log.Warn("counter load failed", "rid", c.rid.String(), "offset", c.offset, "err", err)
			}
		}
		return true
	})
}

// Run is the background goroutine: it runs a (gated) cycle every period and performs a final
// flush when ctx is cancelled (so a graceful Close persists everything).
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
	// CommitPacket strips cancellation internally, so Background is safe here.
	m.states.Range(func(_, v any) bool {
		c := v.(*AtomicCounter)
		if err := c.flush(context.Background()); err != nil && m.log != nil {
			m.log.Warn("counter shutdown flush failed", "rid", c.rid.String(), "offset", c.offset, "err", err)
		}
		return true
	})
}
