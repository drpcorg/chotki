package toyqueue

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTwoWayQueue_Feed(t *testing.T) {
	a, b := BlockingRecordQueuePair(2)
	err := a.Drain(Records{[]byte{'A'}, []byte{'B', 'B'}})
	assert.Nil(t, err)
	go func() {
		time.Sleep(time.Millisecond * 10) // well...
		recs, err := b.Feed()
		assert.Nil(t, err)
		assert.Equal(t, 2, len(recs))
		assert.Equal(t, int64(3), recs.TotalLen())
		recs, err = b.Feed()
		assert.Nil(t, err)
		assert.Equal(t, 1, len(recs))
		assert.Equal(t, int64(3), recs.TotalLen())
	}()
	err = a.Drain(Records{[]byte{'C', 'C', 'C'}})
	assert.Nil(t, err)
}
