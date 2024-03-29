package chotki

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestORMExample(t *testing.T) {
	_ = os.RemoveAll("cho1e")
	_ = os.RemoveAll("cho1f")
	var a, b Chotki
	err := a.Create(0x1e, "test replica")
	assert.Nil(t, err)
	var tid, oid ID
	tid, _ = a.CreateType(ID0, "Sname", "Iscore")
	assert.Equal(t, "1e-1", tid.String())

	oid, _ = a.CreateObject(tid, "\"Ivan Petrov\"", "102")
	assert.Equal(t, "1e-2", oid.String())
	//a.DumpAll()

	err = a.Close()
	assert.Nil(t, err)
	err = a.Open(0x1e)
	assert.Nil(t, err)
	//a.DumpAll()

	var exa Example
	ita := a.ObjectIterator(ParseIDString("1e-2"))
	assert.NotNil(t, ita)
	err = exa.Load(ita)
	assert.Nil(t, err)
	assert.Equal(t, "Ivan Petrov", exa.Name)
	assert.Equal(t, int64(102), exa.Score)

	exa.Score = 103
	// todo save the object

	err = b.Create(0x1f, "another test replica")
	assert.Nil(t, err)

	/* fixme
	a2b, b2a := toyqueue.BlockingRecordQueuePair(1024)
	a.AddPeer(a2b)
	b.AddPeer(b2a)*/
	snap := a.db.NewSnapshot()
	vv, _ := b.VersionVector()
	err = a.SyncPeer(&b, snap, vv)
	assert.Nil(t, err)

	// fixme wait something
	var exb Example
	itb := b.ObjectIterator(ParseIDString("1e-2"))
	assert.NotNil(t, itb)
	err = exb.Load(itb)
	assert.Nil(t, err)
	assert.Equal(t, "Ivan Petrov", exb.Name)
	assert.Equal(t, int64(102), exb.Score)

	err = a.Close()
	assert.Nil(t, err)
	err = b.Close()
	assert.Nil(t, err)
	_ = os.RemoveAll("cho1e")
	_ = os.RemoveAll("cho1f")
}
