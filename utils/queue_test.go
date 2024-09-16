package utils

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBlockingRecordQueue_Drain(t *testing.T) {
	const N = 1 << 10 // 8K
	const K = 1 << 4  // 16

	queue := NewFDQueue[[][]byte](1024, time.Millisecond, 0)

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
