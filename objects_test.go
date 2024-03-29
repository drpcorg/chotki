package chotki

import (
	"os"
	"testing"

	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/stretchr/testify/assert"
)

func TestTypes(t *testing.T) {
	_ = os.RemoveAll("cho1a")
	var a Chotki
	err := a.Create(0x1a, "test replica A")
	assert.Nil(t, err)

	var tid, oid ID
	tid, err = a.CreateType(ID0, "SName", "IScore")
	assert.Nil(t, err)
	oid, _ = a.CreateObject(tid, "\"Petrov\"", "42")
	assert.Equal(t, tid+ProInc, oid)

	//a.DumpAll()

	tid2, fields, err := a.GetObject(oid)
	assert.Nil(t, err)
	assert.Equal(t, tid, tid2)
	assert.Equal(t, 2, len(fields))
	assert.Equal(t, "\"Petrov\"", fields[0])
	assert.Equal(t, "42", fields[1])

	ex := Example{}
	i := a.ObjectIterator(oid)
	assert.NotNil(t, i)
	err = ex.Load(i)
	assert.Nil(t, err)
	assert.Equal(t, "Petrov", ex.Name)
	assert.Equal(t, int64(42), ex.Score)

	i2 := a.ObjectIterator(oid)
	assert.NotNil(t, i2)
	ex.Score = 44
	var changes toyqueue.Records
	changes, err = ex.Store(i2)
	assert.Nil(t, err)
	var eid ID
	eid, err = a.CommitPacket('E', oid, changes)
	assert.Nil(t, err)
	assert.Equal(t, oid+ProInc, eid)

	ex2 := Example{}
	i3 := a.ObjectIterator(oid)
	assert.NotNil(t, i3)
	err = ex2.Load(i3)
	assert.Nil(t, err)
	assert.Equal(t, "Petrov", ex2.Name)
	assert.Equal(t, int64(44), ex2.Score)

	_ = i.Close()
	_ = i2.Close()
	_ = i3.Close()

	_ = a.Close()
	_ = os.RemoveAll("cho1a")
}
