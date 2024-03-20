package main

import (
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestORMExample(t *testing.T) {
	_ = os.RemoveAll("cho1e")
	var a, b Chotki
	err := a.Create(0x1e, "test replica")
	assert.Nil(t, err)
	var tid, oid ID
	tid, err = a.CreateType(ID0, "Sname", "Iscore")
	assert.Equal(t, "1e-1", tid.String())
	oid, err = a.CreateObject(tid, "Ivan Petrov", "102")
	assert.Equal(t, "1e-2", oid.String())

	err = a.Close()
	assert.Nil(t, err)
	err = a.Open(0x1e)
	assert.Nil(t, err)

	var exa Example
	ita := a.ObjectIterator(ParseIDString("1e-2"))
	assert.NotNil(t, ita)
	err = exa.Load(ita)
	assert.Nil(t, err)
	assert.Equal(t, "Ivan Petrov", exa.Name)
	assert.Equal(t, 102, exa.Score)

	exa.Score = 103
	// todo save the object

	err = b.Create(0x1, "another test replica")
	assert.Nil(t, err)

	a2b, b2a := toyqueue.BlockingRecordQueuePair(1024)
	a.AddPeer(a2b)
	b.AddPeer(b2a)

	// fixme wait something
	var exb Example
	itb := b.ObjectIterator(ParseIDString("1e-2"))
	assert.NotNil(t, itb)
	err = exb.Load(itb)
	assert.Nil(t, err)
	assert.Equal(t, "Ivan Petrov", exb.Name)
	assert.Equal(t, 103, exb.Score)

	err = a.Close()
	assert.Nil(t, err)
	err = b.Close()
	assert.Nil(t, err)
	_ = os.RemoveAll("cho14")
	_ = os.Remove("cho14.log")
}
