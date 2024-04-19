package toyqueue

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlockingRecordQueue_Drain(t *testing.T) {
	const N = 1 << 10 // 8K
	const K = 1 << 4  // 16

	orig := RecordQueue{Limit: 1024}
	queue := orig.Blocking()

	for k := 0; k < K; k++ {
		go func(k int) {
			i := uint64(k) << 32
			for n := uint64(0); n < N; n++ {
				var b [8]byte
				binary.LittleEndian.PutUint64(b[:], i|n)
				err := queue.Drain(Records{b[:]})
				assert.Nil(t, err)
			}
		}(k)
	}

	check := [K]int{}
	for i := uint64(0); i < N*K; {
		nums, err := queue.Feed()
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
	err := queue.Drain(recs)
	assert.Equal(t, ErrClosed, err)
	_, err2 := queue.Feed()
	assert.Equal(t, ErrClosed, err2)

}

func TestTwoWayQueue_Drain(t *testing.T) {
	a, b := BlockingRecordQueuePair(1)
	recs := Records{{'a'}}
	go func() {
		err := a.Drain(recs)
		assert.Nil(t, err)
	}()
	recs2, err := b.Feed()
	assert.Nil(t, err)
	assert.Equal(t, recs, recs2)
}
