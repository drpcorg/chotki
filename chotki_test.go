package chotki

import (
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestChotki_Debug(t *testing.T) {
	oid := IDFromSrcSeqOff(0x1e, 0x1ab, 0)
	key := OKey(oid+1, 'I')
	value := Itlv(-13)
	str := ChotkiKVString(key, value)
	assert.Equal(t, "1e-1ab-1.I:\t-13", string(str))

	skey := OKey(oid+2, 'S')
	svalue := Stlv("funny\tstring\n")
	sstr := ChotkiKVString(skey, svalue)
	assert.Equal(t, "1e-1ab-2.S:\t\"funny\\tstring\\n\"", string(sstr))
}

func TestChotki_Create(t *testing.T) {
	_ = os.RemoveAll("cho1a")
	var a Chotki
	err := a.Create(0x1a, "test replica")
	assert.Nil(t, err)
	//a.DumpAll()
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
	//a.DumpAll()
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
	//_, _ = fmt.Fprintln(os.Stderr, "--- synced vv ---")
	//DumpVPacket(vpack)
	_ = vpack

	err = b.Drain(recs)
	assert.Nil(t, err)
	//b.DumpAll()
	bvv, err := b.VersionVector()
	assert.Nil(t, err)
	assert.Equal(t, "1,1c-0-1,1d-0-1", bvv.String())

	_ = a.Close()
	_ = b.Close()
	_ = os.RemoveAll("cho1c")
	_ = os.RemoveAll("cho1d")
}
