package chotki

import (
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"testing"
)

func TestChotki_Debug(t *testing.T) {
	oid := rdx.IDFromSrcSeqOff(0x1e, 0x1ab, 0)
	key := OKey(oid+1, 'I')
	value := rdx.Itlv(-13)
	str := ChotkiKVString(key, value)
	assert.Equal(t, "1e-1ab-1.I:\t-13", string(str))

	skey := OKey(oid+2, 'S')
	svalue := rdx.Stlv("funny\tstring\n")
	sstr := ChotkiKVString(skey, svalue)
	assert.Equal(t, "1e-1ab-2.S:\t\"funny\\tstring\\n\"", string(sstr))
}

func TestChotki_Create(t *testing.T) {
	dirname := ReplicaDirName(0x1a)
	_ = os.RemoveAll(dirname)
	a, exists, err := Open(0x1a, "test replica", dirname)
	assert.Nil(t, err)
	assert.Equal(t, exists, false)
	//a.DumpAll()
	_ = a.Close()
	_ = os.RemoveAll(dirname)
}

type KVMerger interface {
	Merge(key, value []byte) error
}

func TestChotki_Sync(t *testing.T) {
	adir, bdir := ReplicaDirName(0xa), ReplicaDirName(0xb)
	_ = os.RemoveAll(adir)
	_ = os.RemoveAll(bdir)

	a, _, err := Open(0xa, "test replica A", adir)
	assert.Nil(t, err)
	//a.DumpAll()

	b, _, err := Open(0xb, "test replica B", bdir)
	assert.Nil(t, err)

	synca := Syncer{Host: a, Mode: SyncRW, Name: "a"}
	syncb := Syncer{Host: b, Mode: SyncRW, Name: "b"}
	err = toyqueue.Relay(&syncb, &synca)
	assert.Nil(t, err)
	err = toyqueue.Pump(&synca, &syncb)
	assert.Equal(t, io.EOF, err)

	bvv, err := b.VersionVector()
	assert.Nil(t, err)
	assert.Equal(t, "0-2-3,a-0-1,b-0-1", bvv.String())

	b.DumpAll()

	_ = a.Close()
	_ = b.Close()
	_ = os.RemoveAll("choa")
	_ = os.RemoveAll("chob")
}
