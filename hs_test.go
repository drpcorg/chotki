package main

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestHandshake(t *testing.T) {
	_ = os.RemoveAll("cho2")
	_ = os.RemoveAll("cho3")

	var chotki2 Chotki
	chotki2.opts.RelaxedOrder = true
	err := chotki2.Open(2)
	assert.Nil(t, err)

	var chotki3 Chotki
	chotki3.opts.RelaxedOrder = true
	err = chotki3.Open(3)
	assert.Nil(t, err)

	u := Uint64(123)
	state := u.Diff(nil)
	objid := chotki2.NewID()
	packet := MakeObjectPacket(objid, ZeroId, state)
	err = chotki2.AbsorbBatch(Batch{packet})
	assert.Nil(t, err)

	address := "localhost:1234"

	err = chotki2.tcp.Listen(address)
	assert.Nil(t, err)

	err = chotki3.tcp.Connect(address)
	assert.Nil(t, err)

	it := chotki3.ObjectIterator(objid)
	assert.True(t, it.Next())
	replstate := it.Value()
	assert.Equal(t, state, replstate)
	u2 := Uint64(0)
	u2.Apply(replstate)
	assert.Equal(t, uint64(u2), uint64(123))

	err = chotki2.Close()
	assert.Nil(t, err)

	err = chotki3.Close()
	assert.Nil(t, err)

	_ = os.RemoveAll("cho2")
	_ = os.RemoveAll("cho3")
}
