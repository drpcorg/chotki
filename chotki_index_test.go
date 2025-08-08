package chotki

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/drpcorg/chotki/chotki_errors"
	"github.com/drpcorg/chotki/classes"
	"github.com/drpcorg/chotki/rdx"
	testutils "github.com/drpcorg/chotki/test_utils"
	"github.com/drpcorg/chotki/utils"
	"github.com/stretchr/testify/assert"
)

var SchemaIndex = []classes.Field{
	{Name: "test", RdxType: rdx.String, Index: classes.HashIndex},
}

func TestFullScanIndexSync(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()

	a, err := Open(dirs[0], Options{
		Src:                0xa,
		Name:               "test replica A",
		ReadAccumTimeLimit: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer a.Close()

	b, err := Open(dirs[1], Options{
		Src:                0xb,
		Name:               "test replica B",
		ReadAccumTimeLimit: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer b.Close()

	cid, err := a.NewClass(context.Background(), rdx.ID0, Schema...)
	assert.NoError(t, err)

	aorm := a.ObjectMapper()
	defer aorm.Close()
	ob1 := Test{Test: "test1"}
	err = aorm.New(context.Background(), cid, &ob1)
	assert.NoError(t, err)

	aorm.UpdateAll()

	data := make([]Test, 0)

	for item := range SeekClass[*Test](aorm, cid) {
		data = append(data, *item)
	}
	assert.Equal(t, []Test{{Test: "test1"}}, data, "index in sync check after local update")
	testutils.SyncData(a, b)

	borm := b.ObjectMapper()
	defer borm.Close()

	data = make([]Test, 0)
	for item := range SeekClass[*Test](borm, cid) {
		data = append(data, *item)
	}
	assert.Equal(t, []Test{{Test: "test1"}}, data, "index in sync check after diff sync")

	err = a.net.Listen("tcp://127.0.0.1:34934")
	assert.NoError(t, err)

	err = b.net.Connect("tcp://127.0.0.1:34934")
	assert.NoError(t, err)

	time.Sleep(time.Second * 1)

	ob2 := Test{Test: "test2"}
	err = borm.New(context.Background(), cid, &ob2)
	assert.NoError(t, err)
	borm.UpdateAll()

	time.Sleep(time.Millisecond * 100)

	data = make([]Test, 0)
	for item := range SeekClass[*Test](borm, cid) {
		data = append(data, *item)
	}
	assert.Equal(t, []Test{{Test: "test1"}, {Test: "test2"}}, data, "index in sync check after live sync")
}

func TestHashIndexSyncCreateObject(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()

	a, err := Open(dirs[0], Options{
		Src:                0xa,
		Name:               "test replica A",
		Logger:             utils.NewDefaultLogger(slog.LevelDebug),
		ReadAccumTimeLimit: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer a.Close()

	b, err := Open(dirs[1], Options{
		Src:                0xb,
		Name:               "test replica B",
		Logger:             utils.NewDefaultLogger(slog.LevelDebug),
		ReadAccumTimeLimit: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer b.Close()

	cid, err := a.NewClass(context.Background(), rdx.ID0, SchemaIndex...)
	assert.NoError(t, err)

	// sync data before creating object
	testutils.SyncData(a, b)

	aorm := a.ObjectMapper()
	defer aorm.Close()
	ob1 := Test{Test: "test1"}
	err = aorm.New(context.Background(), cid, &ob1)
	assert.NoError(t, err)
	id := aorm.FindID(&ob1)
	assert.NotEqual(t, rdx.BadId, id, "object id should be set")

	aorm.UpdateAll()

	test1data, err := GetByHash[*Test](aorm, cid, 1, []byte("test1"))
	assert.NoError(t, err)
	assert.Equal(t, &Test{Test: "test1"}, test1data, "index in sync check after local update")

	testutils.SyncData(a, b)

	borm := b.ObjectMapper()
	defer borm.Close()

	test1data, err = GetByHash[*Test](borm, cid, 1, []byte("test1"))
	assert.NoError(t, err)
	assert.Equal(t, &Test{Test: "test1"}, test1data, "index in sync check after diff sync")

	err = a.net.Listen("tcp://127.0.0.1:34934")
	assert.NoError(t, err)

	err = b.net.Connect("tcp://127.0.0.1:34934")
	assert.NoError(t, err)

	time.Sleep(time.Second * 1)

	ob2 := Test{Test: "test2"}
	err = borm.New(context.Background(), cid, &ob2)
	assert.NoError(t, err)
	borm.UpdateAll()

	time.Sleep(time.Millisecond * 200)
	aorm.UpdateAll()
	test2data, err := GetByHash[*Test](aorm, cid, 1, []byte("test2"))
	assert.NoError(t, err)
	assert.Equal(t, &Test{Test: "test2"}, test2data, "index in sync check after live sync")
}

func TestHashIndexSyncEditObject(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()

	a, err := Open(dirs[0], Options{
		Src:                0xa,
		Name:               "test replica A",
		Logger:             utils.NewDefaultLogger(slog.LevelDebug),
		ReadAccumTimeLimit: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer a.Close()

	b, err := Open(dirs[1], Options{
		Src:                0xb,
		Name:               "test replica B",
		Logger:             utils.NewDefaultLogger(slog.LevelDebug),
		ReadAccumTimeLimit: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer b.Close()

	cid, err := a.NewClass(context.Background(), rdx.ID0, SchemaIndex...)
	assert.NoError(t, err)

	aorm := a.ObjectMapper()
	defer aorm.Close()
	ob1 := Test{Test: "test1"}
	err = aorm.New(context.Background(), cid, &ob1)
	assert.NoError(t, err)
	id := aorm.FindID(&ob1)
	assert.NotEqual(t, rdx.BadId, id, "object id should be set")

	aorm.UpdateAll()

	// sync data before creating object
	testutils.SyncData(a, b)

	test1data, err := GetByHash[*Test](aorm, cid, 1, []byte("test1"))
	assert.NoError(t, err)
	assert.Equal(t, &Test{Test: "test1"}, test1data, "index in sync after local object create")

	ob1.Test = "test10"
	err = aorm.Save(context.Background(), &ob1)
	assert.NoError(t, err)
	aorm.UpdateAll()

	test1data, err = GetByHash[*Test](aorm, cid, 1, []byte("test10"))
	assert.NoError(t, err)
	assert.Equal(t, &Test{Test: "test10"}, test1data, "index in sync after local object edit")
	_, err = GetByHash[*Test](aorm, cid, 1, []byte("test1"))
	assert.Error(t, chotki_errors.ErrObjectUnknown, err)

	testutils.SyncData(a, b)

	borm := b.ObjectMapper()
	defer borm.Close()

	test1data, err = GetByHash[*Test](borm, cid, 1, []byte("test10"))
	assert.NoError(t, err)
	assert.Equal(t, &Test{Test: "test10"}, test1data, "index in sync check after diff sync")

	err = a.net.Listen("tcp://127.0.0.1:34934")
	assert.NoError(t, err)

	err = b.net.Connect("tcp://127.0.0.1:34934")
	assert.NoError(t, err)

	time.Sleep(time.Second * 1)

	test1data.Test = "test11"
	borm.Save(context.Background(), test1data)
	borm.UpdateAll()

	time.Sleep(time.Millisecond * 200)
	aorm.UpdateAll()
	test1data, err = GetByHash[*Test](aorm, cid, 1, []byte("test11"))
	assert.NoError(t, err)
	assert.Equal(t, &Test{Test: "test11"}, test1data, "index in sync check after live sync")
}

func TestHashIndexRepairIndex(t *testing.T) {
	dirs, clear := testdirs(0xa, 0xb)
	defer clear()

	a, err := Open(dirs[0], Options{
		Src:                0xa,
		Name:               "test replica A",
		Logger:             utils.NewDefaultLogger(slog.LevelDebug),
		ReadAccumTimeLimit: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer a.Close()

	b, err := Open(dirs[1], Options{
		Src:                0xb,
		Name:               "test replica B",
		Logger:             utils.NewDefaultLogger(slog.LevelDebug),
		ReadAccumTimeLimit: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer b.Close()

	cid, err := a.NewClass(context.Background(), rdx.ID0, SchemaIndex...)
	assert.NoError(t, err)

	aorm := a.ObjectMapper()
	defer aorm.Close()
	ob1 := Test{Test: "test1"}
	err = aorm.New(context.Background(), cid, &ob1)
	assert.NoError(t, err)
	id := aorm.FindID(&ob1)
	assert.NotEqual(t, rdx.BadId, id, "object id should be set")

	aorm.UpdateAll()

	testutils.SyncData(a, b)
	time.Sleep(time.Second * 1)

	borm := b.ObjectMapper()
	defer borm.Close()
	test1data, err := GetByHash[*Test](borm, cid, 1, []byte("test1"))
	assert.NoError(t, err)
	assert.Equal(t, &Test{Test: "test1"}, test1data, "index in sync check after diff sync")
}

func TestHashIndexUniqueConstraint(t *testing.T) {
	dirs, clear := testdirs(0xa)
	defer clear()

	a, err := Open(dirs[0], Options{
		Src:                0xa,
		Name:               "test replica A",
		Logger:             utils.NewDefaultLogger(slog.LevelDebug),
		ReadAccumTimeLimit: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	defer a.Close()

	cid, err := a.NewClass(context.Background(), rdx.ID0, SchemaIndex...)
	assert.NoError(t, err)

	aorm := a.ObjectMapper()
	defer aorm.Close()

	// Create first object
	ob1 := Test{Test: "test1"}
	err = aorm.New(context.Background(), cid, &ob1)
	assert.NoError(t, err)
	aorm.UpdateAll()

	// Try to create second object with the same indexed field value
	ob2 := Test{Test: "test1"}
	err = aorm.New(context.Background(), cid, &ob2)
	assert.Error(t, err, "should fail when creating object with duplicate indexed field value")
	assert.ErrorIs(t, err, chotki_errors.ErrHashIndexUinqueConstraintViolation)

	// Verify only one object exists
	data := make([]Test, 0)
	for item := range SeekClass[*Test](aorm, cid) {
		data = append(data, *item)
	}
	assert.Equal(t, 1, len(data), "should only have one object")
	assert.Equal(t, "test1", data[0].Test, "should have the first object's value")
}
