package chotki

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/assert"
)

type FeedCloserTest struct{}

func (t *FeedCloserTest) Feed(context.Context) (recs protocol.Records, err error) {
	return protocol.Records{}, nil
}

func (t *FeedCloserTest) Close() error {
	return nil
}

var _ protocol.FeedCloser = (*FeedCloserTest)(nil)

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

	syncSimplex(a, b)

	bvv, err := b.VersionVector()
	assert.Nil(t, err)
	assert.Equal(t, "0-3,a-0-3,b-0-3", bvv.String())

	_ = a.Close()
	_ = b.Close()
}

func TestChotki_SyncEdit(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0xa, Name: "test replica A"})
	assert.Nil(t, err)
	b, err := Open(dirs[1], Options{Src: 0xb, Name: "test replica B"})
	assert.Nil(t, err)

	cid, err := a.NewClass(context.Background(), rdx.ID0, Schema...)
	assert.NoError(t, err)

	obj := &Test{
		Test: "test data",
	}
	orm := a.ObjectMapper()
	err = orm.New(context.Background(), cid, obj)
	assert.NoError(t, err)
	objectId := orm.FindID(obj)
	orm.Close()
	syncSimplex(a, b)

	orm = a.ObjectMapper()
	resa, err := orm.Load(objectId, &Test{})
	assert.NoError(t, err)
	resa.(*Test).Test = "edited text"
	assert.NoError(t, orm.Save(context.Background(), resa))
	syncSimplex(a, b)

	borm := b.ObjectMapper()
	res, err := borm.Load(objectId, &Test{})
	assert.NoError(t, err)
	assert.Equal(t, &Test{Test: "edited text"}, res)
	orm.Close()
	borm.Close()
	_ = a.Close()
	_ = b.Close()
}

func TestChotki_SyncLivePingsOk(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0xa, Name: "test replica A", Logger: utils.NewDefaultLogger(slog.LevelInfo)})
	assert.Nil(t, err)

	b, err := Open(dirs[1], Options{Src: 0xb, Name: "test replica B", Logger: utils.NewDefaultLogger(slog.LevelInfo)})
	assert.Nil(t, err)

	synca := Syncer{
		Host:       a,
		PingPeriod: 100 * time.Millisecond,
		PingWait:   200 * time.Millisecond,
		Mode:       SyncRWLive, Name: "a",
		Src:    a.src,
		log:    utils.NewDefaultLogger(slog.LevelDebug),
		oqueue: &FeedCloserTest{},
	}
	syncb := Syncer{
		Host:       b,
		PingPeriod: 100 * time.Second,
		Mode:       SyncRWLive,
		PingWait:   3 * time.Second,
		Name:       "b",
		Src:        b.src,
		log:        utils.NewDefaultLogger(slog.LevelDebug),
		oqueue:     &FeedCloserTest{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go protocol.PumpCtxCallback(ctx, &synca, &syncb, func() bool {
		return synca.GetFeedState() != SendPing
	})
	go protocol.PumpCtx(ctx, &syncb, &synca)
	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, SendLive, synca.GetFeedState())
	assert.Equal(t, SendLive, syncb.GetFeedState())
	assert.Equal(t, SendDiff, synca.GetDrainState())
	assert.Equal(t, SendDiff, syncb.GetDrainState())

	time.Sleep(time.Millisecond * 110)
	assert.Equal(t, SendPing, synca.GetFeedState())
	go protocol.PumpCtx(ctx, &synca, &syncb)
	time.Sleep(time.Millisecond * 90)

	assert.Equal(t, SendLive, synca.GetFeedState())
	assert.Equal(t, SendLive, syncb.GetFeedState())
	cancel()
	// wait until everything stopped
	time.Sleep(time.Millisecond * 100)
	syncb.Close()
	synca.Close()
	_ = a.Close()
	_ = b.Close()
}

func TestChotki_SyncLivePingsFail(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0xa, Name: "test replica A", Logger: utils.NewDefaultLogger(slog.LevelInfo)})
	assert.Nil(t, err)

	b, err := Open(dirs[1], Options{Src: 0xb, Name: "test replica B", Logger: utils.NewDefaultLogger(slog.LevelInfo)})
	assert.Nil(t, err)

	synca := Syncer{
		Host:       a,
		PingPeriod: 100 * time.Millisecond,
		PingWait:   100 * time.Millisecond,
		Mode:       SyncRWLive, Name: "a",
		Src:    a.src,
		log:    utils.NewDefaultLogger(slog.LevelDebug),
		oqueue: &FeedCloserTest{},
	}
	syncb := Syncer{
		Host:       b,
		PingPeriod: 100 * time.Second,
		Mode:       SyncRWLive,
		PingWait:   3 * time.Second,
		Name:       "b",
		Src:        b.src,
		log:        utils.NewDefaultLogger(slog.LevelDebug),
		oqueue:     &FeedCloserTest{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go protocol.PumpCtxCallback(ctx, &synca, &syncb, func() bool {
		return synca.GetFeedState() != SendPing
	})
	go protocol.PumpCtxCallback(ctx, &syncb, &synca, func() bool {
		return syncb.GetFeedState() != SendPong
	})
	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, SendLive, synca.GetFeedState())
	assert.Equal(t, SendLive, syncb.GetFeedState())
	assert.Equal(t, SendDiff, synca.GetDrainState())
	assert.Equal(t, SendDiff, syncb.GetDrainState())

	time.Sleep(time.Millisecond * 110)
	assert.Equal(t, SendPing, synca.GetFeedState())
	go protocol.PumpCtx(ctx, &synca, &syncb)
	time.Sleep(time.Millisecond * 200)

	assert.Equal(t, SendNone, synca.GetFeedState())
	assert.Equal(t, SendPong, syncb.GetFeedState())
	assert.Equal(t, SendDiff, synca.GetDrainState())
	assert.Equal(t, SendNone, syncb.GetDrainState())

	cancel()
	syncb.Close()
	synca.Close()
	_ = a.Close()
	_ = b.Close()
}

func TestChotki_SyncGlobals(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0xa, Name: "test replica A"})
	assert.Nil(t, err)

	_, tlv, err := a.ObjectFieldTLV(IdNames)
	assert.Nil(t, err)
	delta := rdx.MdeltaTR(tlv, rdx.MapTR{"test": rdx.ID0.ToOff(100)}, nil)
	_, err = a.EditFieldTLV(context.Background(), IdNames, protocol.Record('M', delta))
	assert.Nil(t, err)

	b, err := Open(dirs[1], Options{Src: 0xb, Name: "test replica B"})
	assert.Nil(t, err)

	syncDuplex(a, b)

	names, err := b.MapTRField(IdNames)
	assert.Nil(t, err)
	assert.Equal(t, rdx.ID0.ToOff(100), names["test"])

	_ = a.Close()
	_ = b.Close()
}

func syncSimplex(a, b *Chotki) error {
	synca := Syncer{
		Host:     a,
		Mode:     SyncRW,
		Name:     "a",
		Src:      a.src,
		log:      utils.NewDefaultLogger(slog.LevelDebug),
		PingWait: time.Second,
	}
	syncb := Syncer{
		Host:     b,
		Mode:     SyncRW,
		Name:     "b",
		Src:      b.src,
		log:      utils.NewDefaultLogger(slog.LevelDebug),
		PingWait: time.Second,
	}
	defer syncb.Close()
	defer synca.Close()
	// send handshake from b to a
	err := protocol.Relay(&syncb, &synca)
	if err != nil {
		return err
	}
	// send data a -> b
	return protocol.Pump(&synca, &syncb)

}

func syncDuplex(a, b *Chotki) error {
	err := syncSimplex(a, b)
	if err != nil && err != io.EOF {
		return err
	}
	return syncSimplex(b, a)
}

func TestChotki_Sync3(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb, 0xc)
	defer clear()

	a, err := Open(dirs[0], Options{Src: 0xa, Name: "test replica A"})
	assert.Nil(t, err)

	b, err := Open(dirs[1], Options{Src: 0xb, Name: "test replica B"})
	assert.Nil(t, err)

	c, err := Open(dirs[2], Options{Src: 0xc, Name: "test replica C"})
	assert.Nil(t, err)

	ids := make(map[string]rdx.ID)

	cid, err := a.NewClass(context.Background(), rdx.ID0, Schema...)
	assert.NoError(t, err)

	// sync class a -> b -> c
	assert.Equal(t, io.EOF, syncDuplex(a, b))
	assert.Equal(t, io.EOF, syncDuplex(b, c))

	for _, db := range []*Chotki{a, b, c} {
		obj := &Test{
			Test: fmt.Sprintf("some data for %s", db.opts.Name),
		}
		orm := db.ObjectMapper()
		err = orm.New(context.Background(), cid, obj)
		assert.NoError(t, err)
		ids[db.opts.Name] = orm.FindID(obj)
		orm.Close()
	}

	assert.Equal(t, io.EOF, syncDuplex(b, c))
	assert.Equal(t, io.EOF, syncDuplex(a, b))
	assert.Equal(t, io.EOF, syncDuplex(b, c))

	for _, db := range []*Chotki{a, b, c} {
		orm := db.ObjectMapper()
		for replica, id := range ids {
			res, err := orm.Load(id, &Test{})
			assert.NoError(t, err, fmt.Sprintf("check error to get data from %s on %s", replica, db.opts.Name))
			assert.Equal(t, &Test{Test: fmt.Sprintf("some data for %s", replica)}, res, fmt.Sprintf("check object from %s on %s", replica, db.opts.Name))
		}
		orm.Close()
	}

	_ = a.Close()
	_ = b.Close()
	_ = c.Close()
}

var Schema = []Field{
	{Name: "test", RdxType: rdx.String},
}

type Test struct {
	Test string
}

var ErrInvalidField = errors.New("Invalid field type")
var _ NativeObject = (*Test)(nil)

func (k *Test) Load(off uint64, rdt byte, tlv []byte) error {
	switch off {
	case 1: // Deleted
		if rdt != rdx.String {
			return ErrInvalidField
		}
		k.Test = rdx.Snative(tlv)
	default:
		return ErrUnknownFieldInAType
	}
	return nil
}

func (k *Test) Store(off uint64, rdt byte, old []byte, clock rdx.Clock) (bare []byte, err error) {
	switch off {
	case 1: // Deleted
		if rdt != rdx.String {
			return nil, ErrInvalidField
		} else if old == nil {
			bare = rdx.Stlv(k.Test)
		} else {
			bare = rdx.Sdelta(old, k.Test, clock)
		}
	default:
		return nil, ErrUnknownFieldInAType
	}
	return
}
