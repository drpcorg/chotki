package counters

import (
	"context"
	"errors"
	"sync"
	"testing"

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
	m := NewAtomicCounterManager(h, 0, nil)
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
	m := NewAtomicCounterManager(h, 0, nil)
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
	m := NewAtomicCounterManager(h, 0, nil)
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
