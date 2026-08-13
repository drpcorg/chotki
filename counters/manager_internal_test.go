package counters

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/assert"
)

// mockHost implements the flush/load path of host.Host and records CommitBatch call sizes.
type mockHost struct {
	host.Host
	mu      sync.Mutex
	batches []int
}

func (h *mockHost) Source() uint64 { return 0x1a }

func (h *mockHost) ObjectFieldTLV(fid rdx.ID) (byte, []byte, error) {
	return rdx.Natural, rdx.Ntlv(0), nil
}

func (h *mockHost) CommitBatch(ctx context.Context, edits []host.Edit) error {
	h.mu.Lock()
	h.batches = append(h.batches, len(edits))
	h.mu.Unlock()
	return nil
}

func (h *mockHost) StartSequentialWrite() {}
func (h *mockHost) EndSequentialWrite()   {}

// Cycle splits >maxFlushBatch dirty counters into chunks of at most maxFlushBatch.
func TestFlushChunking(t *testing.T) {
	old := maxFlushBatch
	maxFlushBatch = 2
	defer func() { maxFlushBatch = old }()

	h := &mockHost{}
	m := NewAtomicCounterManager(h, 0, 0, 0, nil)
	for i := 0; i < 5; i++ {
		rid := rdx.IDFromSrcSeqOff(0x1a, uint64(i+1), 0)
		_, err := m.Counter(rid, 1).Increment(context.Background(), 1)
		assert.NoError(t, err)
	}

	m.Cycle(context.Background())

	// 5 changed counters, chunked by 2 -> commits of sizes [2, 2, 1]
	assert.Equal(t, []int{2, 2, 1}, h.batches)
}

// typeMockHost reports a non-counter field type and counts load attempts.
type typeMockHost struct {
	host.Host
	mu    sync.Mutex
	loads int
}

func (h *typeMockHost) Source() uint64 { return 0x1a }
func (h *typeMockHost) ObjectFieldTLV(fid rdx.ID) (byte, []byte, error) {
	h.mu.Lock()
	h.loads++
	h.mu.Unlock()
	return rdx.String, nil, nil // not a counter -> load() returns ErrNotCounter
}
func (h *typeMockHost) CommitBatch(ctx context.Context, edits []host.Edit) error { return nil }
func (h *typeMockHost) StartSequentialWrite()                                    {}
func (h *typeMockHost) EndSequentialWrite()                                      {}
func (h *typeMockHost) loadCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.loads
}

// A field that is not a counter must be loaded once and then negatively cached,
// so repeated Counter() calls and background ticks don't re-hit the DB every
// time. The cache is time-bounded (negCacheTTL), not a permanent
// latch, so a field that later becomes a counter self-heals.
func TestNotACounterIsNegativelyCached(t *testing.T) {
	h := &typeMockHost{}
	m := NewAtomicCounterManager(h, 0, 0, 0, nil)
	rid := rdx.IDFromSrcSeqOff(0x1a, 1, 0)
	for i := 0; i < 5; i++ {
		_ = m.Counter(rid, 1)
	}
	m.Cycle(context.Background())
	assert.Equal(t, 1, h.loadCount(),
		"a not-a-counter field must be loaded once, then served from the negative cache")
}

var errNotSynced = errors.New("not synced yet")

// flakyHost fails the first failCount load attempts (as if the object isn't
// synced yet — NOT a wrong-type error, so it must not be negatively cached),
// then returns a valid counter.
type flakyHost struct {
	host.Host
	mu        sync.Mutex
	calls     int
	failCount int
}

func (h *flakyHost) Source() uint64 { return 0x1a }
func (h *flakyHost) ObjectFieldTLV(fid rdx.ID) (byte, []byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	if h.calls <= h.failCount {
		return 0, nil, errNotSynced
	}
	return rdx.Natural, rdx.Ntlv(0), nil
}
func (h *flakyHost) CommitBatch(ctx context.Context, edits []host.Edit) error { return nil }
func (h *flakyHost) StartSequentialWrite()                                    {}
func (h *flakyHost) EndSequentialWrite()                                      {}

// A transient load error must NOT drop the reload request: accessed is cleared
// only after a successful load, so the next cycle retries.
func TestAccessedSurvivesTransientLoadError(t *testing.T) {
	h := &flakyHost{failCount: 2}
	m := NewAtomicCounterManager(h, 0, 0, 0, nil)
	rid := rdx.IDFromSrcSeqOff(0x1a, 1, 0)
	c := m.Counter(rid, 1)             // load #1 fails (transient)
	_, _ = c.Get(context.Background()) // sets accessed
	assert.True(t, c.accessed.Load())
	m.Cycle(context.Background()) // reload #2 fails; accessed must remain set
	assert.True(t, c.accessed.Load(),
		"accessed must survive a failed reload")
	m.Cycle(context.Background()) // reload #3 succeeds; accessed cleared
	assert.False(t, c.accessed.Load())
}

// mutableHost serves a Natural counter whose DB value can change externally —
// as if remote replicas' increments synced in while this one never touched it.
type mutableHost struct {
	host.Host
	mu    sync.Mutex
	val   uint64
	loads int
}

func (h *mutableHost) Source() uint64 { return 0x1a }
func (h *mutableHost) ObjectFieldTLV(fid rdx.ID) (byte, []byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.loads++
	return rdx.Natural, rdx.Ntlv(h.val), nil
}
func (h *mutableHost) CommitBatch(ctx context.Context, edits []host.Edit) error { return nil }
func (h *mutableHost) StartSequentialWrite()                                    {}
func (h *mutableHost) EndSequentialWrite()                                      {}
func (h *mutableHost) set(v uint64) {
	h.mu.Lock()
	h.val = v
	h.mu.Unlock()
}
func (h *mutableHost) loadCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.loads
}

// A counter loaded once and then left idle must not serve a value frozen at
// load time forever: past counterStaleTTL, Counter() reads through and picks
// up externally-synced DB changes. (The nightly reset scan read day-old zeros
// for keys billed on other replicas and silently skipped them.)
func TestStaleCounterReadsThrough(t *testing.T) {
	h := &mutableHost{val: 5}
	m := NewAtomicCounterManager(h, 0, 50*time.Millisecond, 0, nil)
	rid := rdx.IDFromSrcSeqOff(0x1a, 1, 0)

	v, err := m.Counter(rid, 1).Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(5), v)

	// remote increments sync into the DB while this replica stays idle
	h.set(42)

	// within TTL: cached value served, no extra DB read
	loadsBefore := h.loadCount()
	v, _ = m.Counter(rid, 1).Get(context.Background())
	assert.Equal(t, int64(5), v)
	assert.Equal(t, loadsBefore, h.loadCount(), "fresh counter must not hit the DB")

	time.Sleep(60 * time.Millisecond)

	// past TTL: Counter() reads through before serving
	v, _ = m.Counter(rid, 1).Get(context.Background())
	assert.Equal(t, int64(42), v, "stale counter must read through to the DB")
}

// Deadlines carry jitter so counters loaded together don't expire together.
func TestStaleDeadlineJitter(t *testing.T) {
	m := NewAtomicCounterManager(&mutableHost{}, 0, time.Minute, 30*time.Second, nil)
	seen := map[int64]struct{}{}
	for i := 0; i < 32; i++ {
		lo := time.Now().Add(m.staleTTL).UnixNano()
		d := m.staleDeadline()
		hi := time.Now().Add(m.staleTTL + m.staleJitter).UnixNano()
		assert.GreaterOrEqual(t, d, lo)
		assert.LessOrEqual(t, d, hi)
		seen[d] = struct{}{}
	}
	assert.Greater(t, len(seen), 1, "jitter must vary deadlines")
}

// loadOnceThenFailHost loads successfully once, then errors — a persistently
// unreadable slot under a counter that already holds data.
type loadOnceThenFailHost struct {
	host.Host
	mu    sync.Mutex
	calls int
}

func (h *loadOnceThenFailHost) Source() uint64 { return 0x1a }
func (h *loadOnceThenFailHost) ObjectFieldTLV(fid rdx.ID) (byte, []byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	if h.calls > 1 {
		return 0, nil, errNotSynced
	}
	return rdx.Natural, rdx.Ntlv(7), nil
}
func (h *loadOnceThenFailHost) CommitBatch(ctx context.Context, edits []host.Edit) error { return nil }
func (h *loadOnceThenFailHost) StartSequentialWrite()                                    {}
func (h *loadOnceThenFailHost) EndSequentialWrite()                                      {}
func (h *loadOnceThenFailHost) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// A failed read-through must keep serving the stale value and back off by the
// retry delay — not turn every Counter() call into a locked DB hit.
func TestFailedReadThroughBacksOff(t *testing.T) {
	h := &loadOnceThenFailHost{}
	m := NewAtomicCounterManager(h, 0, 10*time.Millisecond, 0, nil) // period 0 -> retryDelay 1s
	rid := rdx.IDFromSrcSeqOff(0x1a, 1, 0)

	v, err := m.Counter(rid, 1).Get(context.Background()) // load #1 ok
	assert.NoError(t, err)
	assert.Equal(t, int64(7), v)

	time.Sleep(20 * time.Millisecond)

	// stale: read-through attempt #2 fails; old value survives
	v, err = m.Counter(rid, 1).Get(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int64(7), v, "stale value must survive a failed read-through")
	assert.Equal(t, 2, h.callCount())

	// deadline pushed to now+retryDelay: immediate calls skip the DB
	_, _ = m.Counter(rid, 1).Get(context.Background())
	_, _ = m.Counter(rid, 1).Get(context.Background())
	assert.Equal(t, 2, h.callCount(), "failed read-through must back off, not retry per call")
}
