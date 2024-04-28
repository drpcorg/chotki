package utils

import (
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBlockingRecordQueue_Drain(t *testing.T) {
	const N = 1 << 10 // 8K
	const K = 1 << 4  // 16

	queue := NewRecordQueue(1024, time.Millisecond)

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

type sliceFeedDrainer struct {
	data []byte
	res  []byte
}

func (fd *sliceFeedDrainer) Close() error {
	fd.res = append(fd.res, '(')
	fd.res = append(fd.res, fd.data...)
	fd.res = append(fd.res, ')')
	return nil
}

func (fd *sliceFeedDrainer) Drain(recs Records) error {
	for _, rec := range recs {
		fd.data = append(fd.data, rec...)
	}
	return nil
}

func (fd *sliceFeedDrainer) Feed() (recs Records, err error) {
	for i := 0; i < 3 && len(fd.data) > 0; i++ {
		recs = append(recs, fd.data[0:1])
		fd.data = fd.data[1:]
	}
	if len(fd.data) == 0 {
		err = io.EOF
	}
	return
}

func TestPump(t *testing.T) {
	sfd := sliceFeedDrainer{
		data: []byte("Hello world"),
	}
	err := PumpN(&sfd, &sfd, 2)
	assert.Nil(t, err)
	assert.Equal(t, sfd.data, []byte("worldHello "))

	fro := sliceFeedDrainer{
		data: []byte("Hello world"),
	}
	to := sliceFeedDrainer{}
	err = PumpThenClose(&fro, &to)
	assert.Equal(t, err, io.EOF)
	assert.Equal(t, []byte("(Hello world)"), to.res)
}
