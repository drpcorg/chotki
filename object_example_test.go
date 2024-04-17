package chotki

import (
	"io"
	"testing"

	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/stretchr/testify/assert"
)

func TestORMExample(t *testing.T) {
	dirs, clear := testdirs(0x1e, 0x1f)
	defer clear()

	a, err := Open(dirs[0], Options{Orig: 0x1e, Name: "test replica"})
	assert.Nil(t, err)
	tid, err := a.NewClass(rdx.ID0,
		Field{Name: "Name", RdxType: rdx.String},
		Field{Name: "Score", RdxType: rdx.Integer},
	)
	assert.Nil(t, err)
	assert.Equal(t, "1e-1", tid.String())

	oid, _ := a.NewObject(tid, "\"Ivan Petrov\"", "102")
	assert.Equal(t, "1e-2", oid.String())
	//a.DumpAll()

	err = a.Close()
	assert.Nil(t, err)

	a, err = Open(dirs[0], Options{Orig: 0x1e, Name: "test replica"})
	assert.Nil(t, err)

	var exa Example
	ita := a.ObjectIterator(rdx.IDFromString("1e-2"))
	assert.NotNil(t, ita)
	err = exa.Load(ita)
	assert.Nil(t, err)
	assert.Equal(t, "Ivan Petrov", exa.Name)
	assert.Equal(t, int64(102), exa.Score)

	exa.Score = 103
	// todo save the object

	b, err := Open(dirs[1], Options{Orig:0x1f, Name: "another test replica"})
	assert.Nil(t, err)

	syncera := Syncer{Host: a, Mode: SyncRW}
	syncerb := Syncer{Host: b, Mode: SyncRW}
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
