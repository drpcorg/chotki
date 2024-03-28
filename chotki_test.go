package chotki

import (
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/stretchr/testify/assert"
	"io"
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

	synca := Syncer{Host: &a, Mode: SyncRW}
	syncb := Syncer{Host: &b, Mode: SyncRW}
	err = toyqueue.Relay(&syncb, &synca)
	assert.Nil(t, err)
	err = toyqueue.Pump(&synca, &syncb)
	assert.Equal(t, io.EOF, err)

	bvv, err := b.VersionVector()
	assert.Nil(t, err)
	assert.Equal(t, "1,1c-0-1,1d-0-1", bvv.String())

	_ = a.Close()
	_ = b.Close()
	_ = os.RemoveAll("cho1c")
	_ = os.RemoveAll("cho1d")
}
