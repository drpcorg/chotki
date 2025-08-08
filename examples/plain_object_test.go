package examples

import (
	"context"
	"os"
	"testing"

	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/classes"
	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/assert"
)

func TestPlainObjectORM(t *testing.T) {
	_ = os.RemoveAll("cho10")
	defer os.RemoveAll("cho10")

	a, err := chotki.Open("cho10", chotki.Options{Src: 0x10, Name: "test replica A"})
	assert.Nil(t, err)
	orma := a.ObjectMapper()

	tid, err := a.NewClass(context.Background(), rdx.ID0,
		classes.Field{Name: "Name", RdxType: rdx.String},
		classes.Field{Name: "Group", RdxType: rdx.Reference},
		classes.Field{Name: "Score", RdxType: rdx.Natural},
	)
	assert.Nil(t, err)
	sidorov := Student{
		Name:  "Semen Sidorov",
		Group: rdx.ID0,
		Score: 123,
	}
	// todo StudentClassId -- init from a file (codegen it?)
	err = orma.New(context.Background(), tid, &sidorov)
	assert.Nil(t, err)

	id := orma.FindID(&sidorov)
	assert.NotEqual(t, rdx.BadId, id)
	orma.Close()
	//a.DumpAll()
	_ = a.Close()

	a2, err := chotki.Open("cho10", chotki.Options{Src: 0x10, Name: "test replica A"})
	assert.Nil(t, err)
	a2.DumpAll(os.Stderr)
	orma2 := a2.ObjectMapper()

	sidorov2 := Student{}
	ret, err := orma2.Load(id, &sidorov2)
	assert.Nil(t, err)
	assert.Equal(t, &sidorov2, ret)

	assert.Equal(t, "Semen Sidorov", sidorov2.Name)
	assert.Equal(t, rdx.ID0, sidorov2.Group)
	assert.Equal(t, uint64(123), sidorov2.Score)

	sidorov2.Score = 124
	err = orma2.Save(context.Background(), &sidorov2)
	assert.Nil(t, err)
	assert.Equal(t, uint64(124), sidorov2.Score)
	a2.DumpAll(os.Stderr)

	orma2.Close()
	_ = a2.Close()

	a3, err := chotki.Open("cho10", chotki.Options{Src: 0x10, Name: "test replica A"})
	assert.Nil(t, err)
	a3.DumpAll(os.Stderr)
	orma3 := a3.ObjectMapper()

	sidorov3 := Student{}
	ret, err = orma3.Load(id, &sidorov3)
	assert.Nil(t, err)
	assert.Equal(t, &sidorov3, ret)

	assert.Equal(t, "Semen Sidorov", sidorov3.Name)
	assert.Equal(t, rdx.ID0, sidorov3.Group)
	assert.Equal(t, uint64(124), sidorov3.Score)

}
