package chotki

import (
	"io"
	"os"
	"testing"

	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/stretchr/testify/assert"
)

func TestORMExample(t *testing.T) {
	_ = os.RemoveAll("cho1e")
	_ = os.RemoveAll("cho1f")
	defer func() {
		_ = os.RemoveAll("cho1e")
		_ = os.RemoveAll("cho1f")
	}()

	var a, b Chotki
	err := a.Create(0x1e, "test replica")
	assert.Nil(t, err)
	var tid, oid rdx.ID
	tid, err = a.NewClass(rdx.ID0,
		Field{Name: "Name", RdxType: rdx.String},
		Field{Name: "Score", RdxType: rdx.Integer},
	)
	assert.Nil(t, err)
	assert.Equal(t, "1e-1", tid.String())

	oid, _ = a.NewObject(tid, "\"Ivan Petrov\"", "102")
	assert.Equal(t, "1e-2", oid.String())
	//a.DumpAll()

	err = a.Close()
	assert.Nil(t, err)
	err = a.Open(0x1e)
	assert.Nil(t, err)
	//a.DumpAll()

	var exa Example
	ita := a.ObjectIterator(rdx.IDFromString("1e-2"))
	assert.NotNil(t, ita)
	err = exa.Load(ita)
	assert.Nil(t, err)
	assert.Equal(t, "Ivan Petrov", exa.Name)
	assert.Equal(t, int64(102), exa.Score)

	exa.Score = 103
	// todo save the object

	err = b.Create(0x1f, "another test replica")
	assert.Nil(t, err)

	syncera := Syncer{Host: &a, Mode: SyncRW}
	syncerb := Syncer{Host: &b, Mode: SyncRW}
	err = toyqueue.Relay(&syncerb, &syncera)
	assert.Nil(t, err)
	err = toyqueue.Pump(&syncera, &syncerb)
	assert.Equal(t, io.EOF, err)

	itb := b.ObjectIterator(rdx.IDFromString("1e-2"))
	assert.NotNil(t, itb)

	var exb Example
	err = exb.Load(itb)
	assert.Nil(t, err)

	assert.Equal(t, "Ivan Petrov", exb.Name)
	assert.Equal(t, int64(102), exb.Score)

	assert.Nil(t, ita.Close())
	assert.Nil(t, itb.Close())
	assert.Nil(t, syncera.Close())
	assert.Nil(t, syncerb.Close())
	assert.Nil(t, a.Close())
	assert.Nil(t, b.Close())
}
