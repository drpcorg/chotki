package chotki

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/assert"
)

func TestAtomicCounter(t *testing.T) {
	dir, err := os.MkdirTemp("", "*")
	assert.NoError(t, err)

	a, err := Open(dir, Options{
		Src:     0x1a,
		Name:    "test replica",
		Options: pebble.Options{ErrorIfExists: true},
	})
	assert.NoError(t, err)

	cid, err := a.NewClass(context.Background(), rdx.ID0, Field{Name: "test", RdxType: rdx.Natural})
	assert.NoError(t, err)

	rid, err := a.NewObjectTLV(context.Background(), cid, protocol.Records{protocol.Record('N', rdx.Ntlv(0))})
	assert.NoError(t, err)

	counterA := NewAtomicCounter(a, rid, 1, 0)
	counterB := NewAtomicCounter(a, rid, 1, 0)

	res, err := counterA.Increment(context.Background(), 1)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, res)

	res, err = counterB.Increment(context.Background(), 1)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, res)

	res, err = counterA.Increment(context.Background(), 1)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, res)
}

func TestAtomicCounterWithPeriodicUpdate(t *testing.T) {
	dira, err := os.MkdirTemp("", "*")
	assert.NoError(t, err)

	a, err := Open(dira, Options{
		Src:     0x1a,
		Name:    "test replica",
		Options: pebble.Options{ErrorIfExists: true},
	})
	assert.NoError(t, err)

	dirb, err := os.MkdirTemp("", "*")
	assert.NoError(t, err)

	b, err := Open(dirb, Options{
		Src:     0x1b,
		Name:    "test replica2",
		Options: pebble.Options{ErrorIfExists: true},
	})
	assert.NoError(t, err)

	cid, err := a.NewClass(
		context.Background(), rdx.ID0,
		Field{Name: "test", RdxType: rdx.Natural},
		Field{Name: "test2", RdxType: rdx.ZCounter},
	)
	assert.NoError(t, err)

	rid, err := a.NewObjectTLV(
		context.Background(), cid,
		protocol.Records{
			protocol.Record('N', rdx.Ntlv(0)),
			protocol.Record('Z', rdx.Ztlv(0)),
		},
	)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 1; i <= 2; i++ {

		counterA := NewAtomicCounter(a, rid, uint64(i), 100*time.Millisecond)
		counterB := NewAtomicCounter(b, rid, uint64(i), 0)

		// first increment
		res, err := counterA.Increment(ctx, 1)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, res, fmt.Sprintf("iteration  %d", i))

		syncData(a, b)

		// increment from another replica
		res, err = counterB.Increment(ctx, 1)
		assert.NoError(t, err)
		assert.EqualValues(t, 2, res, fmt.Sprintf("iteration  %d", i))

		syncData(a, b)

		// this increment does not account data from other replica because current value is cached
		res, err = counterA.Increment(ctx, 1)
		assert.NoError(t, err)
		assert.EqualValues(t, 2, res, fmt.Sprintf("iteration  %d", i))

		time.Sleep(100 * time.Millisecond)

		// after wait we increment, and we get actual value
		res, err = counterA.Increment(ctx, 1)
		assert.NoError(t, err)
		assert.EqualValues(t, 4, res, fmt.Sprintf("iteration  %d", i))
	}
}
