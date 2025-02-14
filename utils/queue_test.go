package utils

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBlockingRecordQueue_Drain(t *testing.T) {
	const N = 1 << 10 // 8K
	const K = 1 << 4  // 16

	queue := NewFDQueue[[][]byte](1024, 100*time.Millisecond, 0)

	for k := 0; k < K; k++ {
		go func(k int) {
			i := uint64(k) << 32
			for n := uint64(0); n < N; n++ {
				var b [8]byte
				binary.LittleEndian.PutUint64(b[:], i|n)
				err := queue.Drain(context.Background(), [][]byte{b[:]})
				assert.Nil(t, err)
			}
		}(k)
	}

	check := [K]int{}
	for i := uint64(0); i < N*K; {
		nums, err := queue.Feed(context.Background())
		assert.Nil(t, err)
		for _, num := range nums {
			assert.Equal(t, 8, len(num))
			j := binary.LittleEndian.Uint64(num)
			k := int(j >> 32)
			n := int(j & 0xffffffff)
			assert.Equal(t, check[k], n)
			check[k] = n + 1
			i++
		}
	}

	recs := [][]byte{{'a'}}
	assert.Nil(t, queue.Close())
	err := queue.Drain(context.Background(), recs)
	assert.Equal(t, ErrClosed, err)
	_, err2 := queue.Feed(context.Background())
	assert.Equal(t, ErrClosed, err2)
}

func TestNewFDQueue(t *testing.T) {
	queue := NewFDQueue[[][]byte](10, time.Second, 5)
	assert.NotNil(t, queue, "FDQueue creation failed")
	assert.Equal(t, 0, queue.Size(), "Expected queue length to be 0")
}

func TestFDQueue_DrainAndFeed(t *testing.T) {
	queue := NewFDQueue[[][]byte](10, time.Second, 5)
	ctx := context.Background()

	records := [][]byte{{1}, {2}, {3}, {4}, {5}}
	err := queue.Drain(ctx, records)
	assert.NoError(t, err, "Unexpected error in Drain")

	result, err := queue.Feed(ctx)
	assert.NoError(t, err, "Unexpected error in Feed")
	assert.Equal(t, len(records), len(result), "Mismatch in record length")
	assert.Equal(t, records, result, "Mismatch in records")
}

func TestFDQueue_Close(t *testing.T) {
	queue := NewFDQueue[[][]byte](10, time.Second, 5)
	err := queue.Close()
	assert.NoError(t, err, "Unexpected error during Close")

	err = queue.Drain(context.Background(), [][]byte{{1}, {2}, {3}})
	assert.ErrorIs(t, err, ErrClosed, "Expected ErrClosed after Close")

	_, err = queue.Feed(context.Background())
	assert.ErrorIs(t, err, ErrClosed, "Expected ErrClosed after Close")

	assert.Equal(t, 0, queue.Size(), "Expected queue length to be 0 after close")
}

func TestFDQueue_ConcurrentDrainAndFeed(t *testing.T) {
	queue := NewFDQueue[[][]byte](15, time.Second, 10)
	ctx := context.Background()

	records := [][]byte{{1}, {2}, {3}, {4}, {5}}
	wg := sync.WaitGroup{}

	// Add 15 elements to the queue in 3 batches
	for i := 0; i < 3; i++ {
		err := queue.Drain(ctx, records)
		assert.NoError(t, err, "Unexpected error in Drain")
	}

	// Fetch batches of elements from the queue
	wg.Add(1)
	go func() {
		defer wg.Done()
		// First batch should return 10 elements (batch size of queue)
		result, err := queue.Feed(ctx)
		assert.NoError(t, err, "Unexpected error in Feed")
		assert.Equal(t, 10, len(result), "Expected 10 elements in first batch")
		assert.Equal(t, append(records, records...), result, "Mismatch in first batch records")

		// Second batch should return 5 elements (remaining items)
		result, err = queue.Feed(ctx)
		assert.NoError(t, err, "Unexpected error in Feed")
		assert.Equal(t, 5, len(result), "Expected 5 elements in second batch")
		assert.Equal(t, records, result, "Mismatch in second batch records")
	}()

	wg.Wait()
}

func TestFDQueue_TimeLimit(t *testing.T) {
	queue := NewFDQueue[[][]byte](10, 50*time.Millisecond, 5)
	ctx := context.Background()

	go func() {
		time.Sleep(100 * time.Millisecond)
		queue.Drain(ctx, [][]byte{{1}, {2}, {3}})
	}()

	result, err := queue.Feed(ctx)
	assert.NoError(t, err, "Unexpected error in Feed")
	assert.Equal(t, 0, len(result), "Expected 0 records due to timeout")
}

func TestFDQueue_DrainStopsWhenContextCancelled(t *testing.T) {
	queue := NewFDQueue[[][]byte](10, time.Second, 5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	records := [][]byte{{1}, {2}, {3}, {4}, {5}}
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	queue.Drain(ctx, records)
}

func TestFDQueue_FeedStopsWhenContextCancelled(t *testing.T) {
	queue := NewFDQueue[[][]byte](10, time.Second, 5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	recs, _ := queue.Feed(ctx)
	assert.Nil(t, recs)
}

func TestFDQueue_CloseStopsDrainAndFeed(t *testing.T) {
	queue := NewFDQueue[[][]byte](10, time.Second, 5)
	ctx := context.Background()

	go func() {
		time.Sleep(50 * time.Millisecond)
		queue.Close()
	}()

	records := [][]byte{{1}, {2}, {3}}
	queue.Drain(ctx, records)
	queue.Feed(ctx)
}

func TestFDQueue_Size(t *testing.T) {
	queue := NewFDQueue[[][]byte](10, time.Second, 2)
	ctx := context.Background()

	records := [][]byte{{1, 2}, {2}, {3}}
	queue.Drain(ctx, records)
	assert.Equal(t, queue.Size(), 4)
	_, err := queue.Feed(ctx)
	assert.NoError(t, err)
	assert.Equal(t, queue.Size(), 2)
}

func TestFDQueue_SizeWithWait(t *testing.T) {
	queue := NewFDQueue[[][]byte](10, time.Second, 4)
	ctx := context.Background()

	records := [][]byte{{1, 2}}
	queue.Drain(ctx, records)
	assert.Equal(t, queue.Size(), 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err := queue.Feed(ctx)
		wg.Done()
		assert.NoError(t, err)
	}()
	time.Sleep(time.Millisecond)
	queue.Drain(ctx, records)
	wg.Wait()
	assert.Equal(t, 0, queue.Size())
}

func TestFDQueue_ChannelLimitBlockingBehavior(t *testing.T) {
	queue := NewFDQueue[[][]byte](5, time.Second, 5)
	ctx := context.Background()

	records := [][]byte{{1}, {2}, {3}, {4}, {5}}
	wg := sync.WaitGroup{}

	// Add more elements than the channel limit to test blocking behavior
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			err := queue.Drain(ctx, records)
			assert.NoError(t, err, "Unexpected error in Drain %d", i)
		}
	}()

	// Read elements from the queue to allow the above to proceed
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			result, err := queue.Feed(ctx)
			assert.NoError(t, err, "Unexpected error in Feed %d", i)
			assert.Equal(t, len(records), len(result), "Mismatch in record length %d", i)
			assert.Equal(t, records, result, "Mismatch in records %d", i)
		}
	}()

	wg.Wait()
}
