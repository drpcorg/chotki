package chotki

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/assert"
)

func testdirs(origs ...uint64) ([]string, func()) {
	dirs := make([]string, len(origs))

	for i, orig := range origs {
		dirs[i] = fmt.Sprintf("cho%x", orig)
		os.RemoveAll(dirs[i])
	}

	return dirs, func() {
		for _, dir := range dirs {
			os.RemoveAll(dir)
		}
	}
}

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
	dirs, cancel := testdirs(0x1a)
	defer cancel()

	a, err := Open(dirs[0], Options{
		Src:     0x1a,
		Name:    "test replica",
		Options: pebble.Options{ErrorIfExists: true},
	})
	assert.Nil(t, err)
	assert.NotNil(t, a)

	_ = a.Close()
}

type KVMerger interface {
	Merge(key, value []byte) error
}

func TestChotki_Sync(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0xa, Name: "test replica A"})
	assert.Nil(t, err)
	b, err := Open(dirs[1], Options{Src: 0xb, Name: "test replica B"})
	assert.Nil(t, err)

	synca := Syncer{Host: a, Mode: SyncRW, Name: "a", Log: utils.NewDefaultLogger(slog.LevelDebug)}
	syncb := Syncer{Host: b, Mode: SyncRW, Name: "b", Log: utils.NewDefaultLogger(slog.LevelDebug)}
	err = utils.Relay(&syncb, &synca)
	assert.Nil(t, err)
	err = utils.Pump(&synca, &syncb)
	assert.Equal(t, io.EOF, err)

	bvv, err := b.VersionVector()
	assert.Nil(t, err)
	assert.Equal(t, "0-2-3,a-0-1,b-0-1", bvv.String())

	b.DumpAll()

	_ = a.Close()
	_ = b.Close()
}
