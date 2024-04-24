package examples

import (
	"io"
	"os"
	"testing"

	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/toyqueue"
	"github.com/stretchr/testify/assert"
)

func TestORMExample(t *testing.T) {
	defer os.RemoveAll("cho1e")
	defer os.RemoveAll("cho1f")

	a, err := chotki.Open("cho1e", chotki.Options{Src: 0x1e, Name: "test replica"})
	assert.Nil(t, err)
	tid, err := a.NewClass(rdx.ID0,
		chotki.Field{Name: "Name", RdxType: rdx.String},
		chotki.Field{Name: "Score", RdxType: rdx.Integer},
	)
	assert.Nil(t, err)
	assert.Equal(t, "1e-1", tid.String())

	oid, _ := a.NewObject(tid, "\"Ivan Petrov\"", "102")
	assert.Equal(t, "1e-2", oid.String())
	//a.DumpAll()

	err = a.Close()
	assert.Nil(t, err)

	a, err = chotki.Open("cho1e", chotki.Options{Src: 0x1e, Name: "test replica"})
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

	b, err := chotki.Open("cho1f", chotki.Options{Src: 0x1f, Name: "another test replica"})
	assert.Nil(t, err)

	syncera := chotki.Syncer{Host: a, Mode: chotki.SyncRW}
	syncerb := chotki.Syncer{Host: b, Mode: chotki.SyncRW}
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
