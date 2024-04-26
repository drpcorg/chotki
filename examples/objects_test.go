package examples

import (
	"os"
	"testing"

	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/assert"
)

func TestTypes(t *testing.T) {
	defer os.RemoveAll("cho1a")

	a, err := chotki.Open("cho1a", chotki.Options{Src: 0x1a, Name: "test replica A"})
	assert.Nil(t, err)

	var tid, oid rdx.ID
	tid, err = a.NewClass(rdx.ID0,
		chotki.Field{Name: "Name", RdxType: rdx.String},
		chotki.Field{Name: "Score", RdxType: rdx.Integer},
	)
	assert.Nil(t, err)
	oid, err = a.NewObject(tid, "\"Petrov\"", "42")
	assert.Nil(t, err)
	assert.Equal(t, tid+rdx.ProInc, oid)

	//a.DumpAll()

	tid2, decl, fields, err := a.ObjectFields(oid)
	assert.Nil(t, err)
	assert.Equal(t, tid, tid2)
	assert.Equal(t, 3, len(fields))
	assert.Equal(t, "Petrov", rdx.Snative(fields[1]))
	assert.Equal(t, int64(42), rdx.Inative(fields[2]))
	assert.Equal(t, decl[1].Name, "Name")
	assert.Equal(t, decl[1].RdxType, rdx.String)
	assert.Equal(t, decl[2].Name, "Score")
	assert.Equal(t, decl[2].RdxType, rdx.Integer)

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
	var changes utils.Records
	changes, err = ex.Store(i2)
	assert.Nil(t, err)
	var eid rdx.ID
	eid, err = a.CommitPacket('E', oid, changes)
	assert.Nil(t, err)
	assert.Equal(t, oid+rdx.ProInc, eid)

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
}
