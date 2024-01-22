package main

import (
	"github.com/learn-decentralized-systems/toytlv"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestChotki_AbsorbBatch(t *testing.T) {
	_ = os.RemoveAll("cho14")
	_ = os.Remove("cho14.log")
	var chotki Chotki
	chotki.opts.RelaxedOrder = true
	err := chotki.Open(14)
	assert.Nil(t, err)

	typeid := ParseID("8e-5e84")
	oid := ParseID("8e-82f0")
	name := []byte("John Brown")

	packet := toytlv.Concat(
		toytlv.Record('O',
			toytlv.Record('I', oid.ZipBytes()),
			toytlv.Record('R', typeid.ZipBytes()),
			toytlv.Record('R',
				ID(ExampleName).ZipBytes()),
			toytlv.Record('S',
				toytlv.Record('T', ZipUint64(0)),
				name,
			),
			toytlv.Record('R',
				ID(ExampleScore).ZipBytes()),
			toytlv.Record('C',
				ZipUint64(ZigZagInt64(99)),
			),
		),
	)

	err = chotki.AbsorbBatch(Batch{packet})
	assert.Nil(t, err)

	clr, err := chotki.log.Reader(0)
	assert.Nil(t, err)
	logbuf := make([]byte, len(packet))
	n, err := clr.Read(logbuf)
	assert.Nil(t, err)
	assert.Equal(t, len(packet), n)
	assert.Equal(t, packet, logbuf)

	f1id := oid.ToOff(ExampleName)
	key := OKey(f1id)
	val1, _, err := chotki.db.DB.Get(key)
	assert.Nil(t, err)
	assert.Equal(t,
		toytlv.Concat(
			toytlv.Record('I', oid.ToOff(1).ZipBytes()),
			toytlv.Record('S',
				toytlv.Record('T', ZipUint64(0)),
				name,
			),
		),
		val1)

	f2id := oid.ToOff(ExampleScore)
	val2, _, err := chotki.db.DB.Get(OKey(f2id)) // fixme closer
	assert.Nil(t, err)
	assert.Equal(t,
		toytlv.Concat(
			toytlv.Record('I', oid.ToOff(2).ZipBytes()),
			toytlv.Record('C', ZipUint64(ZigZagInt64(99))),
		),
		val2)

	iter := chotki.ObjectIterator(oid)
	assert.NotNil(t, iter)
	var ex Example
	err = ex.Apply(iter)
	assert.Nil(t, err)
	assert.Equal(t, string(name), string(ex.Name))
	assert.Equal(t, uint64(99), uint64(ex.Score))
	_ = iter.Close()

	edits := Batch{
		toytlv.Record('E',
			toytlv.Record('I', ParseID("8f-204").ZipBytes()),
			toytlv.Record('R', ParseID("8e-82f0-42").ZipBytes()),
			toytlv.Record('C', ZipUint64(ZigZagInt64(1))),
		),
		toytlv.Record('E',
			toytlv.Record('I', ParseID("11-7f2").ZipBytes()),
			toytlv.Record('R', ParseID("8e-82f0-32").ZipBytes()),
			toytlv.Record('S',
				toytlv.Record('T', ZipUint64(1)),
				[]byte("John F. Brown"),
			),
		),
	}

	err = chotki.AbsorbBatch(edits)
	assert.Nil(t, err)

	johnfbrown := toytlv.Concat(
		toytlv.Record('I', ParseID("11-7f2-1").ZipBytes()),
		toytlv.Record('S',
			toytlv.Record('T', ZipUint64(1)),
			[]byte("John F. Brown"),
		),
	)

	state100 := toytlv.Concat(
		toytlv.Record('I', ParseID("8f-204-1").ZipBytes()),
		toytlv.Record('C', ZipZagInt64(1)),
		toytlv.Record('I', oid.ToOff(2).ZipBytes()),
		toytlv.Record('C', ZipZagInt64(99)),
	)

	val1b, err := chotki.db.Get('O', oid.ToOff(ExampleName).String583())
	assert.Nil(t, err)
	assert.Equal(t,
		johnfbrown,
		[]byte(val1b))

	val2b, err := chotki.db.Get('O', oid.ToOff(ExampleScore).String583())
	assert.Nil(t, err)
	assert.Equal(t,
		state100,
		[]byte(val2b))

	err = chotki.Close()
	assert.Nil(t, err)
	_ = os.RemoveAll("cho14")
	_ = os.Remove("cho14.log")
}
