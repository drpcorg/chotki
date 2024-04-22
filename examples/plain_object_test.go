package examples

import (
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestPlainObjectORM(t *testing.T) {
	defer os.RemoveAll("cho10")

	a, err := chotki.Open("cho10", chotki.Options{Src: 0x10, Name: "test replica A"})
	assert.Nil(t, err)
	orma := a.ObjectMapper()

	tid, err := a.NewClass(rdx.ID0,
		chotki.Field{Name: "Name", RdxType: rdx.String},
		chotki.Field{Name: "Group", RdxType: rdx.Reference},
		chotki.Field{Name: "Score", RdxType: rdx.Natural},
	)
	assert.Nil(t, err)
	sidorov := Student{
		Name:  "Semen Sidorov",
		Group: rdx.ID0,
		Score: 123,
	}
	// todo StudentClassId -- init from a file (codegen it?)
	err = orma.New(tid, &sidorov)
	assert.Nil(t, err)

	id := orma.FindID(&sidorov)
	assert.NotEqual(t, rdx.BadId, id)

	//a.DumpAll()
	_ = a.Close()

	a2, err := chotki.Open("cho10", chotki.Options{Src: 0x10, Name: "test replica A"})
	assert.Nil(t, err)
	//a2.DumpAll()
	orma2 := a2.ObjectMapper()

	sidorov2 := Student{}
	ret, err := orma2.Load(id, &sidorov2)
	assert.Nil(t, err)
	assert.Equal(t, &sidorov2, ret)

	assert.Equal(t, "Semen Sidorov", sidorov2.Name)
	assert.Equal(t, rdx.ID0, sidorov2.Group)
	assert.Equal(t, uint64(123), sidorov2.Score)

}
