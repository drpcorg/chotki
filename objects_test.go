package chotki

import (
	"context"
	"os"
	"testing"

	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/stretchr/testify/assert"
)

func TestChotkiMapTRField(t *testing.T) {
	_ = os.RemoveAll("cho1")
	defer os.RemoveAll("cho1")

	a, err := Open("cho1", Options{Src: 0x1, Name: "test replica 1"})
	assert.Nil(t, err)

	oid, err := a.NewObjectTLV(context.Background(), rdx.ID0+1, protocol.Records{
		protocol.Record('M'),
	})
	assert.Nil(t, err)
	fid := oid.ToOff(1)

	tr := rdx.MapTR{
		"Name0": rdx.ID0 + 100,
		"Name1": rdx.ID0 + 1,
		"Name3": rdx.ID0 + 3,
	}
	id1, err := a.AddToMapTRField(context.Background(), fid, tr)
	assert.Nil(t, err)
	dtr := rdx.MapTR{
		"Name1": rdx.ID0 + 1,
		"Name2": rdx.ID0 + 2,
		"Name3": rdx.ID0,
	}
	id2, err := a.AddToMapTRField(context.Background(), fid, dtr)
	assert.Nil(t, err)
	assert.NotEqual(t, id1, id2)

	correct := rdx.MapTR{
		"Name0": rdx.ID0 + 100,
		"Name1": rdx.ID0 + 1,
		"Name2": rdx.ID0 + 2,
	}
	merged, err := a.MapTRField(fid)
	assert.Nil(t, err)
	assert.Equal(t, correct, merged)

	id3, err := a.SetMapTRField(context.Background(), fid, correct)
	assert.Nil(t, err)
	assert.Equal(t, rdx.ID0, id3)

	_ = a.Close()
}

func TestChotkiMapSSField(t *testing.T) {
	_ = os.RemoveAll("cho2")
	defer os.RemoveAll("cho2")

	a, err := Open("cho2", Options{Src: 0x2, Name: "test replica 2"})
	assert.Nil(t, err)

	oid, err := a.NewObjectTLV(context.Background(), rdx.ID0+1, protocol.Records{
		protocol.Record('M'),
	})
	assert.Nil(t, err)
	fid := oid.ToOff(1)

	tr := rdx.MapSS{
		"Name0": "Value0",
		"Name1": "Value1",
		"Name3": "Value3",
	}
	id1, err := a.AddToMapSSField(context.Background(), fid, tr)
	assert.Nil(t, err)
	dtr := rdx.MapSS{
		"Name1": "Value1",
		"Name2": "Value2",
		"Name3": "",
	}
	id2, err := a.AddToMapSSField(context.Background(), fid, dtr)
	assert.Nil(t, err)
	assert.NotEqual(t, id1, id2)

	correct := rdx.MapSS{
		"Name0": "Value0",
		"Name1": "Value1",
		"Name2": "Value2",
	}
	merged, err := a.MapSSField(fid)
	assert.Nil(t, err)
	assert.Equal(t, correct, merged)

	id3, err := a.SetMapSSField(context.Background(), fid, correct)
	assert.Nil(t, err)
	assert.Equal(t, rdx.ID0, id3)

	_ = a.Close()
}

func TestChotki_SetMapSSField(t *testing.T) {
	_ = os.RemoveAll("cho3")
	defer os.RemoveAll("cho3")

	a, err := Open("cho3", Options{Src: 0x3, Name: "test replica 3"})
	assert.Nil(t, err)

	oid, err := a.NewObjectTLV(context.Background(), rdx.ID0+1, protocol.Records{
		protocol.Record('M'),
	})
	assert.Nil(t, err)
	fid := oid.ToOff(1)

	state1 := rdx.MapSS{
		"A": "1",
		"B": "2",
		"C": "3",
	}

	id1, err := a.SetMapSSField(context.Background(), fid, state1)
	assert.Nil(t, err)
	assert.NotEqual(t, rdx.ID0, id1)

	state2 := rdx.MapSS{
		"A": "1",
		"B": "22",
		"D": "4",
	}

	id2, err := a.SetMapSSField(context.Background(), fid, state2)
	assert.Nil(t, err)
	assert.NotEqual(t, rdx.ID0, id2)

	result, err := a.MapSSField(fid)
	assert.Nil(t, err)
	assert.Equal(t, state2, result)

	_ = a.Close()
}
