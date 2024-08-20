package chotki

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/protocol"
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
	str := dumpKVString(key, value)
	assert.Equal(t, "1e-1ab-1.I:\t-13", string(str))

	skey := OKey(oid+2, 'S')
	svalue := rdx.Stlv("funny\tstring\n")
	sstr := dumpKVString(skey, svalue)
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

	synca := Syncer{Host: a, Mode: SyncRW, Name: "a", log: utils.NewDefaultLogger(slog.LevelDebug)}
	syncb := Syncer{Host: b, Mode: SyncRW, Name: "b", log: utils.NewDefaultLogger(slog.LevelDebug)}
	err = protocol.Relay(&syncb, &synca)
	assert.Nil(t, err)
	err = protocol.Pump(&synca, &syncb)
	assert.Equal(t, io.EOF, err)

	bvv, err := b.VersionVector()
	assert.Nil(t, err)
	assert.Equal(t, "0-3,a-0-3,b-0-3", bvv.String())

	b.DumpAll(os.Stderr)

	_ = synca.Close()
	_ = syncb.Close()
	_ = a.Close()
	_ = b.Close()
}

var Schema = []Field{
	{Name: "test", RdxType: rdx.Integer},
}

type Test struct {
	Test int64 `chotki:"test"`
}

var ErrInvalidField = errors.New("Invalid field type")
var _ NativeObject = (*Test)(nil)

func (k *Test) Load(off uint64, rdt byte, tlv []byte) error {
	switch off {
	case 1: // Deleted
		if rdt != rdx.Integer {
			return ErrInvalidField
		}
		k.Test = rdx.Inative(tlv)
	default:
		return ErrUnknownFieldInAType
	}
	return nil
}

func (k *Test) Store(off uint64, rdt byte, old []byte, clock rdx.Clock) (bare []byte, err error) {
	switch off {
	case 1: // Deleted
		if rdt != rdx.Integer {
			return nil, ErrInvalidField
		} else if old == nil {
			bare = rdx.Itlv(k.Test)
		} else {
			bare = rdx.Idelta(old, k.Test, clock)
		}
	default:
		return nil, ErrUnknownFieldInAType
	}
	return
}
