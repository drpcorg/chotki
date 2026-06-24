package counters

import (
	"context"
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
