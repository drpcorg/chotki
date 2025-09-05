package indexes

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/chotki_errors"
	"github.com/drpcorg/chotki/classes"
	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/rdx"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
)

var ReindexTaskCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "chotki",
	Subsystem: "index_manager",
	Name:      "reindex_tasks",
}, []string{"class", "field", "reason"})

var ReindexTaskStates = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "chotki",
	Subsystem: "index_manager",
	Name:      "reindex_tasks_states",
}, []string{"class", "field"})

var ReindexCount = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "chotki",
	Subsystem: "index_manager",
	Name:      "reindex",
}, []string{"class", "field"})

var ReindexResults = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "chotki",
	Subsystem: "index_manager",
	Name:      "reindex_results",
}, []string{"class", "field", "result", "type"})

var ReindexDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "chotki",
	Subsystem: "index_manager",
	Name:      "reindex_duration",
	Buckets:   []float64{0, 1, 5, 10, 20, 50, 100, 200, 500},
}, []string{"class", "field"})

type reindexTaskState byte

const (
	reindexTaskStatePending    reindexTaskState = 'P'
	reindexTaskStateInProgress reindexTaskState = 'I'
	reindexTaskStateDone       reindexTaskState = 'D'
	reindexTaskStateRemove     reindexTaskState = 'R'
)

type IndexManager struct {
	c              host.Host
	tasksCancels   map[string]context.CancelFunc
	taskEntries    sync.Map
	mutexMap       sync.Map
	classCache     *lru.Cache[rdx.ID, rdx.ID]
	hashIndexCache *lru.Cache[string, rdx.ID]
}

func NewIndexManager(c host.Host) *IndexManager {
	cache, _ := lru.New[rdx.ID, rdx.ID](10000)
	hashCache, _ := lru.New[string, rdx.ID](100000)
	return &IndexManager{
		c:              c,
		tasksCancels:   make(map[string]context.CancelFunc),
		taskEntries:    sync.Map{},
		classCache:     cache,
		hashIndexCache: hashCache,
	}
}

func fullScanKey(cid rdx.ID, oid rdx.ID) []byte {
	key := []byte{'I', 'F'}
	key = append(key, cid.Bytes()...)
	key = append(key, oid.Bytes()...)
	key = append(key, 'T')
	return key
}

func fullScanKeyId(key []byte) rdx.ID {
	oid := rdx.IDFromBytes(key[18:34])
	return oid
}

func hashKey(cid rdx.ID, fid uint32, hash uint64) []byte {
	key := []byte{'I', 'H'}
	key = append(key, cid.Bytes()...)
	key = binary.BigEndian.AppendUint32(key, fid)
	key = binary.BigEndian.AppendUint64(key, hash)
	key = append(key, 'E')
	return key
}

type ReindexTask struct {
	State      reindexTaskState
	LastUpdate time.Time
	Cid        rdx.ID
	Field      uint32
	Revision   int64
	Src        uint64
}

func (t *ReindexTask) Key() []byte {
	return append(append([]byte{'I', 'T'}, t.Cid.Bytes()...), 'M')
}

func (t *ReindexTask) Id() string {
	return fmt.Sprintf("%s:%d", t.Cid.String(), t.Field)
}

func (t *ReindexTask) Value() []byte {
	mp := rdx.NewStampedMap[rdx.RdxInt, rdx.RdxString]()
	data := []byte{byte(t.State)}
	extime := uint64(t.LastUpdate.Unix())
	data = binary.BigEndian.AppendUint64(data, extime)
	mp.SetStamped(rdx.RdxInt(t.Field), rdx.RdxString(string(data)), rdx.Time{Rev: t.Revision, Src: t.Src})
	return mp.Tlv()
}

func parseReindexTasks(key, value []byte) ([]ReindexTask, error) {
	cid := rdx.IDFromBytes(key[2:18])
	mp := rdx.NewStampedMap[rdx.RdxInt, rdx.RdxString]()
	err := mp.Native(value)
	if err != nil {
		return nil, err
	}
	tasks := []ReindexTask{}
	for k, v := range mp.Map {
		state := reindexTaskState(v.Value[0])
		extime := int64(binary.BigEndian.Uint64([]byte(v.Value[1:])))
		updatetime := time.Unix(extime, 0)
		tasks = append(tasks, ReindexTask{
			State:      state,
			Revision:   v.Time.Rev,
			Src:        v.Time.Src,
			LastUpdate: updatetime,
			Cid:        cid,
			Field:      uint32(k),
		})
	}
	return tasks, nil
}

func (im *IndexManager) AddFullScanIndex(cid rdx.ID, oid rdx.ID, batch *pebble.Batch) error {
	im.classCache.Add(oid, cid)
	return batch.Merge(
		fullScanKey(cid, oid),
		[]byte{},
		im.c.WriteOptions(),
	)
}

func (im *IndexManager) GetByHash(cid rdx.ID, fid uint32, otlv []byte, reader pebble.Reader) (rdx.ID, error) {
	cacheKey := append(binary.BigEndian.AppendUint32(cid.Bytes(), fid), otlv...)
	result, ok := im.hashIndexCache.Get(string(cacheKey))
	if ok {
		return result, nil
	}
	hash := xxhash.Sum64(otlv)
	key := hashKey(cid, fid, hash)
	tlv, closer, err := reader.Get(key)
	if closer != nil {
		defer closer.Close()
	}
	if err == pebble.ErrNotFound {
		return rdx.BadId, chotki_errors.ErrObjectUnknown
	}
	if err != nil {
		return rdx.BadId, err
	}
	set := rdx.NewStampedSet[rdx.RdxRid]()
	err = set.Native(tlv)
	if err != nil {
		return rdx.BadId, err
	}
	for id := range set.Value {
		_, data := im.c.GetFieldTLV(rdx.ID(id).ToOff(uint64(fid)))
		_, _, data = rdx.ParseFIRST(data)
		if bytes.Equal(data, otlv) {
			im.hashIndexCache.Add(string(cacheKey), rdx.ID(id))
			return rdx.ID(id), nil
		}
	}
	return rdx.BadId, chotki_errors.ErrObjectUnknown
}

func (im *IndexManager) SeekClass(cid rdx.ID, reader pebble.Reader) iter.Seq[rdx.ID] {
	return func(yield func(id rdx.ID) bool) {
		iter, err := reader.NewIter(&pebble.IterOptions{
			LowerBound: fullScanKey(cid, rdx.ID{}),
			UpperBound: fullScanKey(cid, rdx.BadId),
		})
		if err != nil {
			return
		}
		defer iter.Close()
		for valid := iter.First(); valid; valid = iter.Next() {
			oid := fullScanKeyId(iter.Key())
			if !yield(oid) {
				return
			}
		}
	}
}

func (im *IndexManager) HandleClassUpdate(id rdx.ID, cid rdx.ID, newFieldsBody []byte) ([]ReindexTask, error) {
	newFields := classes.ParseClass(newFieldsBody)
	return im.HandleClassUpdateParsed(id, cid, newFields, false)
}

func (im *IndexManager) HandleClassUpdateParsed(id rdx.ID, cid rdx.ID, newFields classes.Fields, force bool) ([]ReindexTask, error) {
	tasks := []ReindexTask{}
	for i, newField := range newFields {
		if newField.Index == classes.HashIndex {
			oldFields, err := im.c.ClassFields(cid)
			if err == chotki_errors.ErrTypeUnknown {
				// new class, everything is new, create task
				ReindexTaskCount.WithLabelValues(id.String(), fmt.Sprintf("%d", i), "new_class_same_batch").Inc()
				task := &ReindexTask{
					State:      reindexTaskStatePending,
					Cid:        id,
					Field:      uint32(i),
					Revision:   int64(im.c.Last().Pro()),
					Src:        im.c.Source(),
					LastUpdate: time.Now(),
				}
				tasks = append(tasks, *task)
			} else if err != nil {
				// something went wrong, so we can't proceed with update
				return nil, err
			} else {
				oldField := oldFields.FindName(newField.Name)
				if oldField == -1 {
					// field just created with index, no need to reindex
					continue
				}
				if oldFields[oldField].Index != classes.HashIndex && newField.Index == classes.HashIndex || force {
					ReindexTaskCount.WithLabelValues(id.String(), fmt.Sprintf("%d", i), "created_new_index").Inc()
					task := &ReindexTask{
						State:      reindexTaskStatePending,
						Cid:        cid,
						Field:      uint32(oldField),
						Revision:   int64(im.c.Last().Pro()),
						Src:        im.c.Source(),
						LastUpdate: time.Now(),
					}
					tasks = append(tasks, *task)
				}
				if oldFields[oldField].Index == classes.HashIndex && newField.Index != classes.HashIndex {
					ReindexTaskCount.WithLabelValues(id.String(), fmt.Sprintf("%d", i), "deleted_index").Inc()
					task := &ReindexTask{
						State:      reindexTaskStatePending,
						Cid:        cid,
						Field:      uint32(oldField),
						Revision:   int64(im.c.Last().Pro()),
						Src:        im.c.Source(),
						LastUpdate: time.Now(),
					}
					tasks = append(tasks, *task)
				}
			}
		}
	}
	return tasks, nil
}

func (im *IndexManager) CheckReindexTasks(ctx context.Context) {
	cycle := func() {
		iter, err := im.c.Database().NewIter(&pebble.IterOptions{
			LowerBound: []byte{'I', 'T'},
			UpperBound: []byte{'I', 'U'},
		})
		if err != nil {
			im.c.Logger().ErrorCtx(ctx, "failed to create reindex tasks iterator: %s", err)
			return
		}
		defer iter.Close()
		for valid := iter.First(); valid; valid = iter.Next() {
			tasks, err := parseReindexTasks(iter.Key(), iter.Value())
			if err != nil {
				im.c.Logger().ErrorCtx(ctx, "failed to parse reindex tasks: %s", err)
				continue
			}
			for _, task := range tasks {
				ReindexTaskStates.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field)).Set(float64(task.State))
				switch task.State {
				case reindexTaskStatePending:
					rev, ok := im.taskEntries.LoadOrStore(task.Id(), task.Revision)
					if !ok {
						// task is in progress, but its not working start
						cancel, ok := im.tasksCancels[task.Id()]
						if ok {
							cancel()
						}
						ctx, cancel := context.WithCancel(ctx)
						im.tasksCancels[task.Id()] = cancel
						go im.runReindexTask(ctx, &task)
					} else {
						// task is working, but we need to restart it, because of external event
						if rev.(int64) < task.Revision {
							cancel, ok := im.tasksCancels[task.Id()]
							if ok {
								cancel()
							}
							ctx, cancel := context.WithCancel(ctx)
							im.tasksCancels[task.Id()] = cancel
							im.taskEntries.Store(task.Id(), task.Revision)
							go im.runReindexTask(ctx, &task)
						}
					}
				case reindexTaskStateInProgress:
					// check that task is still running
					_, ok := im.taskEntries.LoadOrStore(task.Id(), task.Revision)
					if !ok {
						cancel, ok := im.tasksCancels[task.Id()]
						if ok {
							cancel()
						}
						ctx, cancel := context.WithCancel(ctx)
						im.taskEntries.Store(task.Id(), task.Revision)
						im.tasksCancels[task.Id()] = cancel
						go im.runReindexTask(ctx, &task)
					}
				case reindexTaskStateRemove:
					// noop
				case reindexTaskStateDone:
					if task.LastUpdate.Before(time.Now().Add(-10 * time.Minute)) {
						task.State = reindexTaskStatePending
						task.Revision++
						task.LastUpdate = time.Now()
						ReindexTaskCount.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "scheduled_reindex").Inc()
						im.c.Database().Merge(task.Key(), task.Value(), im.c.WriteOptions())
					}
				}
			}
		}
	}
	for ctx.Err() == nil {
		cycle()
		time.Sleep(1 * time.Second)
	}
}

func (im *IndexManager) addHashIndex(cid rdx.ID, fid rdx.ID, tlv []byte, batch pebble.Writer) error {
	lock, _ := im.mutexMap.LoadOrStore(fid, &sync.Mutex{})
	mt := lock.(*sync.Mutex)
	mt.Lock()
	defer func() {
		mt.Unlock()
		im.mutexMap.Delete(fid)
	}()
	id, err := im.GetByHash(cid, uint32(fid.Off()), tlv, im.c.Database())
	switch err {
	case nil:
		if id != fid.ZeroOff() {
			return errors.Join(chotki_errors.ErrHashIndexUinqueConstraintViolation, fmt.Errorf("key %s, current id %s, new id %s", string(tlv), id.String(), fid.ZeroOff().String()))
		}
		fallthrough
	case chotki_errors.ErrObjectUnknown:
		cacheKey := append(binary.BigEndian.AppendUint32(cid.Bytes(), uint32(fid.Off())), tlv...)
		im.hashIndexCache.Remove(string(cacheKey))
		hash := xxhash.Sum64(tlv)
		key := hashKey(cid, uint32(fid.Off()), hash)
		set := rdx.NewStampedSet[rdx.RdxRid]()
		set.Add(rdx.RdxRid(fid.ZeroOff()))
		return batch.Merge(
			key,
			set.Tlv(),
			im.c.WriteOptions(),
		)
	default:
		return err
	}
}

func (im *IndexManager) OnFieldUpdate(rdt byte, fid, cid rdx.ID, tlv []byte, batch pebble.Writer) error {
	if !rdx.IsFirst(rdt) {
		return nil
	}
	if cid == rdx.BadId {
		var ok bool
		cid, ok = im.classCache.Get(fid.ZeroOff())
		if !ok {
			_, tlv := im.c.GetFieldTLV(fid.ZeroOff())
			cid = rdx.IDFromZipBytes(tlv)
			if cid != rdx.BadId && cid != rdx.ID0 {
				im.classCache.Add(fid.ZeroOff(), cid)
			}
		}
	}
	// ID0 class is not indexed
	if cid == rdx.ID0 {
		return nil
	}
	fields, err := im.c.ClassFields(cid)
	if err != nil {
		task := &ReindexTask{
			State:      reindexTaskStatePending,
			Cid:        cid,
			Field:      uint32(fid.Off()),
			Revision:   int64(im.c.Last().Pro()),
			Src:        im.c.Source(),
			LastUpdate: time.Now(),
		}
		return batch.Merge(task.Key(), task.Value(), im.c.WriteOptions())
	}
	if int(fid.Off()) >= len(fields) {
		return nil
	}
	field := fields[fid.Off()]
	if field.Index == classes.HashIndex {
		_, _, tlv := rdx.ParseFIRST(tlv)
		return im.addHashIndex(cid, fid, tlv, batch)
	}
	return nil
}

func (im *IndexManager) runReindexTask(ctx context.Context, task *ReindexTask) {
	start := time.Now()
	ReindexCount.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field)).Inc()
	defer im.taskEntries.CompareAndDelete(task.Id(), task.Revision)

	task.State = reindexTaskStateInProgress
	task.LastUpdate = time.Now()
	task.Revision++
	ctx = im.c.Logger().WithDefaultArgs(ctx, "cid", task.Cid.String(), "field", fmt.Sprintf("%d", task.Field), "process", "reindex")
	err := im.c.Database().Merge(task.Key(), task.Value(), im.c.WriteOptions())
	if err != nil {
		ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "error", "fail_to_set_in_progress").Inc()
		im.c.Logger().ErrorCtx(ctx, "failed to set reindex task to in progress: %s, restarting", err)
		return
	}

	fields, err := im.c.ClassFields(task.Cid)
	if err != nil {
		ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "error", "fail_to_get_class_fields").Inc()
		im.c.Logger().ErrorCtx(ctx, "failed to get class fields: %s, will restart", err)
		return
	}

	if int(task.Field) >= len(fields) {
		im.c.Logger().ErrorCtx(ctx, "field out of range, will restart", "field", task.Field, "class", task.Cid.String(), "fields", fields)
		return
	}

	field := fields[task.Field]
	if field.Index == 0 {
		err := im.c.Database().DeleteRange(
			hashKey(task.Cid, uint32(task.Field), 0),
			hashKey(task.Cid, uint32(task.Field), math.MaxUint64),
			im.c.WriteOptions(),
		)
		if err != nil {
			ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "error", "fail_to_delete_hash_index").Inc()
			im.c.Logger().ErrorCtx(ctx, "failed to delete hash index: %s, will restart", err)
			return
		}
		task.State = reindexTaskStateRemove
		task.Revision++
		task.LastUpdate = time.Now()
		err = im.c.Database().Merge(task.Key(), task.Value(), im.c.WriteOptions())
		if err != nil {
			ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "error", "fail_to_save_done_task").Inc()
			im.c.Logger().ErrorCtx(ctx, "failed to save done task: %s, will restart", err)
			return
		}
		ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "success", "deleted_hash_index").Inc()
		return
	}

	// check data in snapshot, because we don't need to index new objects
	snap := im.c.Database().NewSnapshot()
	defer snap.Close()
	// repair index missing objects
	for id := range im.SeekClass(task.Cid, snap) {
		if ctx.Err() != nil {
			return
		}
		fid := id.ToOff(uint64(task.Field))
		rdt, tlv, err := im.c.ObjectFieldTLV(fid)
		if err != nil {
			ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "error", "fail_to_get_object_field_tlv").Inc()
			im.c.Logger().ErrorCtx(ctx, "failed to get object field tlv: %s, will restart", err)
			return
		}
		if !rdx.IsFirst(rdt) {
			ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "error", "object_field_is_not_first").Inc()
			im.c.Logger().ErrorCtx(ctx, "object field is not first, skipping")
			continue
		}
		// unpack FIRST
		_, _, tlv = rdx.ParseFIRST(tlv)
		_, err = im.GetByHash(task.Cid, uint32(fid.Off()), tlv, im.c.Database())
		if err == chotki_errors.ErrObjectUnknown {
			err = im.addHashIndex(task.Cid, fid, tlv, im.c.Database())
			if err != nil {
				ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "error", "fail_to_add_hash_index").Inc()
				im.c.Logger().ErrorCtx(ctx, "failed to add hash index: %s, will restart", err)
				return
			}
		} else if err != nil {
			ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "error", "fail_to_get_object_by_hash").Inc()
			im.c.Logger().ErrorCtx(ctx, "failed to get object by hash: %s, will restart", err)
			return
		}
	}
	// repair index entries that are no longer needed
	indexIter, err := im.c.Database().NewIter(&pebble.IterOptions{
		LowerBound: hashKey(task.Cid, uint32(task.Field), 0),
		UpperBound: hashKey(task.Cid, uint32(task.Field), math.MaxUint64),
	})
	if err != nil {
		ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "error", "fail_to_create_index_iterator").Inc()
		im.c.Logger().ErrorCtx(ctx, "failed to create index iterator: %s, will restart", err)
		return
	}
	defer indexIter.Close()
	for valid := indexIter.First(); valid; valid = indexIter.Next() {
		set := rdx.NewStampedSet[rdx.RdxRid]()
		err := set.Native(indexIter.Value())
		if err != nil {
			ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "error", "fail_to_parse_index_set").Inc()
			im.c.Logger().ErrorCtx(ctx, "failed to parse index set: %s, will restart", err)
			return
		}
		for id := range set.Value {
			rdt, tlv := im.c.GetFieldTLV(rdx.ID(id).ToOff(uint64(task.Field)))
			// index pointing nowhere, delete
			if tlv == nil {
				im.c.Database().Delete(indexIter.Key(), im.c.WriteOptions())
			} else {
				// likely not possible, but delete
				if !rdx.IsFirst(rdt) {
					im.c.Database().Delete(indexIter.Key(), im.c.WriteOptions())
					continue
				}
				_, _, btlv := rdx.ParseFIRST(tlv)
				hash := xxhash.Sum64(btlv)
				indexHash := binary.BigEndian.Uint64(indexIter.Key()[len(indexIter.Key())-9 : len(indexIter.Key())])
				if hash != indexHash {
					// the indexed value has changed, delete
					im.c.Database().Delete(indexIter.Key(), im.c.WriteOptions())
				}
			}
		}
	}
	task.State = reindexTaskStateDone
	task.LastUpdate = time.Now()
	task.Revision++
	err = im.c.Database().Merge(task.Key(), task.Value(), im.c.WriteOptions())
	if err != nil {
		ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "error", "fail_to_save_done_task").Inc()
		im.c.Logger().ErrorCtx(ctx, "failed to save reindex task: %s, will restart", err)
		return
	}
	if ctx.Err() == nil {
		ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "success", "reindexed").Inc()
		ReindexDuration.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field)).Observe(time.Since(start).Seconds())
	} else {
		ReindexResults.WithLabelValues(task.Cid.String(), fmt.Sprintf("%d", task.Field), "cancelled", "cancelled").Inc()
	}
}
