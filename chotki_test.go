package main

import (
	"fmt"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestChotki_Debug(t *testing.T) {
	oid := IDFromSrcSeqOff(0x1e, 0x1ab, 0)
	key := OKey(FieldID(oid, 1, 'I'))
	value := Itlv(-13)
	str := ChotkiKVString(key, value)
	fmt.Printf("%s\n", str)
	assert.Equal(t, "O.1e-1ab-1.I:\t-13", string(str))

	skey := OKey(FieldID(oid, 2, 'S'))
	svalue := Stlv("funny\tstring\n")
	sstr := ChotkiKVString(skey, svalue)
	fmt.Printf("%s\n", sstr)
	assert.Equal(t, "O.1e-1ab-2.S:\t\"funny\\tstring\\n\"", string(sstr))
}

func TestChotki_Create(t *testing.T) {
	_ = os.RemoveAll("cho1a")
	var a Chotki
	err := a.Create(0x1a, "test replica")
	assert.Nil(t, err)
	a.DumpAll()
	_ = a.Close()
	_ = os.RemoveAll("cho1a")
}

type KVMerger interface {
	Merge(key, value []byte) error
}

func TestChotki_Sync(t *testing.T) {
	_ = os.RemoveAll("cho1c")
	_ = os.RemoveAll("cho1d")
	var a, b Chotki
	err := a.Create(0x1c, "test replica A")
	assert.Nil(t, err)
	a.DumpAll()
	err = b.Create(0x1d, "test replica B")
	assert.Nil(t, err)

	queue := toyqueue.RecordQueue{Limit: 1024}
	snap := a.db.NewSnapshot()
	vv := make(VV)
	vv.PutID(IDFromSrcSeqOff(0, 1, 0))
	err = a.SyncPeer(&queue, snap, vv)
	assert.Nil(t, err)
	recs, err := queue.Feed()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(recs)) // one block, one vv
	vpack, err := ParseVPack(recs[1])
	assert.Nil(t, err)
	_, _ = fmt.Fprintln(os.Stderr, "--- synced vv ---")
	DumpVPacket(vpack)

	err = b.Drain(recs)
	assert.Nil(t, err)
	b.DumpAll()
	bvv, err := b.VersionVector()
	assert.Nil(t, err)
	assert.Equal(t, "0,1c-0-32,1d-0-32", bvv.String())

	_ = a.Close()
	_ = b.Close()
	_ = os.RemoveAll("cho1c")
	_ = os.RemoveAll("cho1d")
}

func TestChotki_AbsorbBatch(t *testing.T) {
	_ = os.RemoveAll("cho1e")
	var a, b Chotki
	err := a.Create(0x1e, "test replica")
	assert.Nil(t, err)
	var tid, oid id64
	tid, err = a.CreateType("Example", "Sname", "Iscore")
	assert.Equal(t, "1e-1", tid.String())
	oid, err = a.CreateObject(tid, "Ivan Petrov", "102")
	assert.Equal(t, "1e-2", oid.String())

	err = a.Close()
	assert.Nil(t, err)
	err = a.Open(0x1e)
	assert.Nil(t, err)

	var exa Example
	ita := a.ObjectIterator(ParseIDString("1e-2"))
	assert.NotNil(t, ita)
	err = exa.Load(ita)
	assert.Nil(t, err)
	assert.Equal(t, "Ivan Petrov", exa.Name)
	assert.Equal(t, 102, exa.Score)

	exa.Score = 103
	// todo save the object

	err = b.Create(0x1, "another test replica")
	assert.Nil(t, err)

	a2b, b2a := toyqueue.BlockingRecordQueuePair(1024)
	a.AddPeer(a2b)
	b.AddPeer(b2a)

	// fixme wait something
	var exb Example
	itb := b.ObjectIterator(ParseIDString("1e-2"))
	assert.NotNil(t, itb)
	err = exb.Load(itb)
	assert.Nil(t, err)
	assert.Equal(t, "Ivan Petrov", exb.Name)
	assert.Equal(t, 103, exb.Score)

	err = a.Close()
	assert.Nil(t, err)
	err = b.Close()
	assert.Nil(t, err)
	_ = os.RemoveAll("cho14")
	_ = os.Remove("cho14.log")
}
