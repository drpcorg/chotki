package chotki

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/drpcorg/chotki/chotki_errors"
	"github.com/drpcorg/chotki/classes"
	"github.com/drpcorg/chotki/counters"
	"github.com/drpcorg/chotki/host"
	"github.com/drpcorg/chotki/indexes"
	"github.com/drpcorg/chotki/network"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/replication"
	"github.com/drpcorg/chotki/utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/puzpuzpuz/xsync/v3"
)

var (
	ErrDbClosed       = errors.New("chotki: db is closed")
	ErrDirnameIsFile  = errors.New("chotki: the dirname is file")
	ErrNotImplemented = errors.New("chotki: not implemented yet")
	ErrHookNotFound   = errors.New("chotki: hook not found")
	ErrBadIRecord     = errors.New("chotki: bad id-ref record")
	ErrBadORecord     = errors.New("chotki: bad id-ref record")
	ErrBadEPacket     = errors.New("chotki: bad E packet")
	ErrBadVPacket     = errors.New("chotki: bad V packet")
	ErrBadYPacket     = errors.New("chotki: bad Y packet")
	ErrBadLPacket     = errors.New("chotki: bad L packet")
	ErrBadTPacket     = errors.New("chotki: bad T packet")
	ErrBadOPacket     = errors.New("chotki: bad O packet")
	ErrSrcUnknown     = errors.New("chotki: source unknown")
	ErrSyncUnknown    = errors.New("chotki: sync session unknown")
	ErrBadRRecord     = errors.New("chotki: bad ref record")

	ErrBadTypeDescription  = errors.New("chotki: bad type description")
	ErrUnknownFieldInAType = errors.New("chotki: unknown field for the type")
	ErrBadClass            = errors.New("chotki: bad class description")

	ErrOutOfOrder      = errors.New("chotki: order fail: sequence gap")
	ErrCausalityBroken = errors.New("chotki: order fail: refs an unknown op")
)

var cleanSyncInterval = time.Minute

var EventsMetric = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "chotki",
	Name:      "packet_count",
})
var EventsOutboundMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "chotki",
	Name:      "outbound_packet_count",
}, []string{"name"})

var EventsBatchSize = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "chotki",
	Name:      "batch_size",
	Buckets:   []float64{0, 1, 10, 50, 100, 500, 1000, 10000, 100000, 1000000},
})

var DrainTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "chotki",
	Name:      "drain_time",
	Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 100, 500, 1000, 5000, 10000},
}, []string{"type"})

// Size of diff sync in bytes
var DiffSyncSize = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "chotki",
	Name:      "diff_sync_size",
	Buckets: []float64{
		1, 10, 100, 1000,
		5000, 10000, 50000, 100000, 500000,
		1000000, 2000000, 5000000, 10000000,
		20000000, 50000000, 100000000,
		200000000, 500000000, 1000000000,
		2000000000, 5000000000,
	},
})

type Options struct {
	pebble.Options

	Src                        uint64
	Name                       string
	Log1                       protocol.Records
	Logger                     utils.Logger
	PingPeriod                 time.Duration // how often should we ping neighbour replicae if its silent
	PingWait                   time.Duration // how much time we wait until pong received
	PebbleWriteOptions         *pebble.WriteOptions
	BroadcastQueueMaxSize      int // size in bytes, after reaching it all writes will block
	BroadcastQueueMinBatchSize int // reads will wait until they have enough data or timelimit expires
	// if this limit expires before read has enough data (BroadcastQueueMinBatchSize) it will return whatever it has,
	// writes will cause overflow error which will result in queue shutdown and session end
	BroadcastQueueTimeLimit    time.Duration
	ReadAccumTimeLimit         time.Duration //
	ReadMaxBufferSize          int
	ReadMinBufferSizeToProcess int
	TcpReadBufferSize          int
	TcpWriteBufferSize         int
	WriteTimeout               time.Duration
	TlsConfig                  *tls.Config
	MaxSyncDuration            time.Duration
	CounterSyncPeriod          time.Duration // flush+reload cadence for the counter manager
	CounterStaleTTL            time.Duration // max age of a cached counter before Counter() reads through to the DB
	CounterStaleJitter         time.Duration // random extra per load, spreads read-through deadlines apart
}

func (o *Options) SetDefaults() {
	if o.PingPeriod == 0 {
		o.PingPeriod = 30 * time.Second
	}

	if o.PingWait == 0 {
		o.PingWait = 10 * time.Second
	}

	if o.ReadMaxBufferSize == 0 {
		o.ReadMaxBufferSize = 1024 * 1024 * 1000 // 1000MB
	}
	if o.ReadMinBufferSizeToProcess == 0 {
		o.ReadMinBufferSizeToProcess = 10 * 1024 // 10kb
	}

	if o.PebbleWriteOptions == nil {
		o.PebbleWriteOptions = &pebble.WriteOptions{Sync: true}
	}

	if o.BroadcastQueueTimeLimit == 0 {
		o.BroadcastQueueTimeLimit = time.Second
	}
	if o.BroadcastQueueMaxSize == 0 {
		o.BroadcastQueueMaxSize = 10 * 1024 * 1024 // 10Mb
	}
	if o.ReadAccumTimeLimit == 0 {
		o.ReadAccumTimeLimit = 5 * time.Second
	}

	if o.WriteTimeout == 0 {
		o.WriteTimeout = 5 * time.Minute
	}

	if o.MaxSyncDuration == 0 {
		o.MaxSyncDuration = 10 * time.Minute
	}

	// Run treats <= 0 as "no ticker", so normalize negatives too.
	if o.CounterSyncPeriod <= 0 {
		o.CounterSyncPeriod = time.Second
	}

	if o.CounterStaleTTL <= 0 {
		o.CounterStaleTTL = 5 * time.Minute
	}
	if o.CounterStaleJitter <= 0 {
		o.CounterStaleJitter = 30 * time.Second
	}

	o.Merger = &pebble.Merger{
		Name: "CRDT",
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			target := make([]byte, len(value))
			copy(target, value)
			var rdt byte
			switch key[0] {
			case 'O':
				_, rdt = host.OKeyIdRdt(key)
			case 'V':
				rdt = 'V'
			case 'I':
				rdt = key[len(key)-1]
			}
			pma := PebbleMergeAdaptor{
				rdt:  rdt,
				vals: [][]byte{target},
			}
			return &pma, nil
		},
	}

	if o.Logger == nil {
		o.Logger = utils.NewDefaultLogger(slog.LevelWarn)
	}
}

type Hook func(cho *Chotki, id rdx.ID) error

type CallHook struct {
	hook Hook
	id   rdx.ID
}

type syncPoint struct {
	mu sync.Mutex
	// batch is nilled out under mu once closed or applied,
	// so any later reader can detect this and skip the apply.
	batch *pebble.Batch
	start time.Time
	// via is the id of the replication session whose 'H' record created
	// this sync point; the sync point is aborted when that session ends
	// (AbortSyncsVia), as its remaining 'D'/'V' records die with the
	// session's connection.
	via     string
	created []rdx.ID // objects created in this sync; reindexed from merged state after 'V'
}

// closeLocked closes the batch if it's still open. Must be called with s.mu held.
func (s *syncPoint) closeLocked() {
	if s.batch != nil {
		_ = s.batch.Close()
		s.batch = nil
	}
}

// close acquires s.mu and closes the batch if it's still open.
func (s *syncPoint) close() {
	s.mu.Lock()
	s.closeLocked()
	s.mu.Unlock()
}

// Main Chotki struct
type Chotki struct {
	// last is the cached local id allocator state: CommitPacket stamps
	// the next local mutation from it. It is advanced both by local
	// commits (under commitMutex) and by replication sessions draining
	// records stamped with our own src (e.g. our own history synced back
	// after a restore from an older snapshot) — those paths do not share
	// a lock, so last has its own one (lastLock). An unsynchronized read
	// here can reuse an already issued seq: two different mutations under
	// one rdx.ID (see tla/README.md, bug 4).
	lastLock  sync.Mutex
	last      rdx.ID
	src       uint64
	clock     rdx.Clock
	cancelCtx context.CancelFunc
	waitGroup *sync.WaitGroup

	lock        sync.RWMutex
	commitMutex sync.Mutex
	// seqWriteMu backs Start/EndSequentialWrite (host.Host): the cooperative
	// bracket read-modify-write writers hold across their read AND commit.
	// Deliberately NOT commitMutex — commit methods lock that internally, so
	// the bracket must nest around them (ordering: seqWriteMu → commitMutex).
	seqWriteMu   sync.Mutex
	db           *pebble.DB
	net          *network.Net
	dir          string
	opts         Options
	log          utils.Logger
	counters     *counters.AtomicCounterManager
	IndexManager *indexes.IndexManager

	outq  *xsync.MapOf[string, protocol.DrainCloser] // queues to broadcast all new packets
	syncs *xsync.MapOf[rdx.ID, *syncPoint]
	hooks *xsync.MapOf[rdx.ID, []Hook]
	types *xsync.MapOf[rdx.ID, classes.Fields]
}

// Checks if the directory exists and is a directory.
func Exists(dirname string) (bool, error) {
	stats, err := os.Stat(dirname)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, err
	}

	if !stats.IsDir() {
		return false, ErrDirnameIsFile
	}

	desc, err := pebble.Peek(dirname, vfs.Default)
	if err != nil {
		return false, err
	}

	return desc.Exists, nil
}

// Worket that cleans diff syncs that took too long.
// Otherwise they stay in the memmory forever if can't be finished
func (cho *Chotki) cleanSyncs(ctx context.Context) {
	for ctx.Err() == nil {
		cho.syncs.Range(func(id rdx.ID, s *syncPoint) bool {
			if time.Since(s.start) > cho.opts.MaxSyncDuration {
				s.close()
				cho.syncs.Delete(id)
				cho.log.WarnCtx(ctx, "diff sync took too long", "id", id, "duration", time.Since(s.start))
			}
			return true
		})
		select {
		case <-ctx.Done():
			return
		case <-time.After(cleanSyncInterval):
		}
	}
}

// reindexPeriod is how often the background reindex worker scans for tasks.
// A var so tests can set it long, isolating the apply path from the backstop.
var reindexPeriod = time.Second

// Opens a new Chotki instance.
func Open(dirname string, opts Options) (*Chotki, error) {
	exists, err := Exists(dirname)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("error when checking db directory"), err)
	}

	opts.SetDefaults() // todo param

	db, err := pebble.Open(dirname, &opts.Options)
	if err != nil {
		return nil, errors.Join(err, fmt.Errorf("error opening pebble"))
	}

	absdir, err := filepath.Abs(dirname)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	cho := Chotki{
		db:   db,
		src:  opts.Src,
		dir:  absdir,
		log:  opts.Logger,
		opts: opts,
		// TODO: delete as it is unused actually
		clock: &rdx.LocalLogicalClock{Source: opts.Src},

		outq:      xsync.NewMapOf[string, protocol.DrainCloser](),
		syncs:     xsync.NewMapOf[rdx.ID, *syncPoint](),
		hooks:     xsync.NewMapOf[rdx.ID, []Hook](),
		types:     xsync.NewMapOf[rdx.ID, classes.Fields](),
		cancelCtx: cancel,
		waitGroup: &wg,
	}

	// If Open fails after workers start, the caller has no *Chotki to Close;
	// tear down via Close (nil-safe on a partial instance, single teardown path).
	opened := false
	defer func() {
		if !opened {
			_ = cho.Close()
		}
	}()

	cho.net = network.NewNet(cho.log,
		func(name string) protocol.FeedDrainCloserTraced { // new connection

			queue := utils.NewFDQueue[protocol.Records](cho.opts.BroadcastQueueMaxSize, cho.opts.BroadcastQueueTimeLimit, cho.opts.BroadcastQueueMinBatchSize)
			if q, loaded := cho.outq.LoadAndStore(name, queue); loaded && q != nil {
				cho.log.Warn(fmt.Sprintf("closing the old conn to %s", name))
				if err := q.Close(); err != nil {
					cho.log.Error(fmt.Sprintf("couldn't close conn %s", name), "err", err)
				}
			}

			return &replication.Syncer{
				Src:        cho.src,
				Host:       &cho,
				Mode:       replication.SyncRWLive,
				PingPeriod: cho.opts.PingPeriod,
				PingWait:   cho.opts.PingWait,
				Name:       name,
				Log:        cho.log,
				Oqueue:     queue,
			}
		},
		func(name string, p protocol.Traced) { // destroy connection
			if q, deleted := cho.outq.LoadAndDelete(name); deleted && q != nil {
				cho.log.Warn(fmt.Sprintf("closing the old conn to %s", name), "trace_id", p.GetTraceId())
				if err := q.Close(); err != nil {
					cho.log.Error(fmt.Sprintf("couldn't close conn %s", name), "err", err, "trace_id", p.GetTraceId())
				}
				cho.log.Warn(fmt.Sprintf("closed the old conn to %s", name), "trace_id", p.GetTraceId())
			}
		},
		&network.NetTlsConfigOpt{Config: opts.TlsConfig},
		&network.NetReadBatchOpt{
			ReadAccumTimeLimit: cho.opts.ReadAccumTimeLimit,
			BufferMaxSize:      cho.opts.ReadMaxBufferSize,
			BufferMinToProcess: cho.opts.ReadMinBufferSizeToProcess,
		},
		&network.TcpBufferSizeOpt{Read: cho.opts.TcpReadBufferSize, Write: cho.opts.TcpWriteBufferSize},
		&network.NetWriteTimeoutOpt{Timeout: cho.opts.WriteTimeout},
	)
	cho.IndexManager = indexes.NewIndexManager(&cho)
	wg.Add(1)
	// reindex tasks are checked in a separate worker
	reindexP := reindexPeriod
	go func() {
		defer wg.Done()
		cho.IndexManager.CheckReindexTasks(ctx, reindexP)
	}()

	wg.Add(1)
	// diff syncs are cleaned up in a separate worker
	go func() {
		defer wg.Done()
		cho.cleanSyncs(ctx)
	}()

	cho.counters = counters.NewAtomicCounterManager(&cho, opts.CounterSyncPeriod, opts.CounterStaleTTL, opts.CounterStaleJitter, cho.log)
	wg.Add(1)
	// counters are periodically flushed and reloaded in a separate worker
	go func() {
		defer wg.Done()
		cho.counters.Run(ctx)
	}()

	if !exists {
		id0 := rdx.IDFromSrcSeqOff(opts.Src, 0, 0)
		// apply log0, some default objects for all replicas
		init := append(protocol.Records(nil), Log0...)
		// log1 is parameter that allows to define some default objects for the replica
		init = append(init, opts.Log1...)
		// creates a replica object, however it is now unused
		init = append(init, protocol.Record('Y',
			protocol.Record('I', id0.ZipBytes()),
			protocol.Record('R', rdx.ID0.ZipBytes()),
			protocol.Record('S', rdx.Stlv(opts.Name)),
			protocol.Record('V', []byte{}),
			protocol.Record('S', rdx.Stlv("")),
		))

		if _, err = cho.drain(context.Background(), init, nil); err != nil {
			return nil, errors.Join(err, fmt.Errorf("unable to drain initial data to chotki"))
		}
	}

	vv, err := cho.VersionVector()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("unable to get version vector"), err)
	}

	cho.last = vv.GetID(cho.src)

	opened = true
	return &cho, nil
}

// Gracefully closes the Chotki instance.
func (cho *Chotki) Close() error {
	cho.lock.Lock()
	defer cho.lock.Unlock()

	if cho.cancelCtx != nil {
		cho.cancelCtx()
		cho.cancelCtx = nil
		cho.waitGroup.Wait()
	}

	if cho.net != nil {
		if err := cho.net.Close(); err != nil {
			cho.log.Error("couldn't close network", "err", err)
		}

		cho.net = nil
	}
	// commitMutex lets an in-flight local commit (which holds it, not cho.lock)
	// finish its drain + reindex reads on cho.db before we close and nil it.
	// Network drains are guarded by cho.lock; the shutdown flush already ran.
	cho.commitMutex.Lock()
	if cho.db != nil {
		if err := cho.db.Close(); err != nil {
			cho.log.Error("couldn't close Pebble", "err", err)
		}

		cho.db = nil
	}
	cho.commitMutex.Unlock()

	cho.outq.Clear()
	cho.syncs.Range(func(id rdx.ID, s *syncPoint) bool {
		s.close()
		return true
	})
	cho.syncs.Clear()
	cho.hooks.Clear()
	cho.types.Clear()

	cho.src = 0
	cho.lastLock.Lock()
	cho.last = rdx.ID0
	cho.lastLock.Unlock()

	return nil
}

// Counter returns a lock-free handle for the field; increments are flushed periodically and on Close (hard crash loses them).
func (cho *Chotki) Counter(rid rdx.ID, offset uint64) *counters.AtomicCounter {
	return cho.counters.Counter(rid, offset)
}

// SyncCounters forces an immediate flush+reload cycle of all counters.
func (cho *Chotki) SyncCounters(ctx context.Context) {
	cho.counters.Cycle(ctx)
}

// ObjectIDByHash resolves an object's id from a hash-indexed first-field value (e.g. a
// UUID) without allocating a snapshot: it serves from the IndexManager's hash cache
// (invalidated as objects change) and reads the live DB only on a miss. For hot paths
// that need just the id, not a consistent object read.
func (cho *Chotki) ObjectIDByHash(cid rdx.ID, fid uint32, fieldValue []byte) (rdx.ID, error) {
	return cho.IndexManager.GetByHash(cid, fid, fieldValue, cho.db)
}

// Returns the source id of the Chotki instance.
func (cho *Chotki) Source() uint64 {
	return cho.src
}

// Returns the clock of the Chotki instance.
func (cho *Chotki) Clock() rdx.Clock {
	return cho.clock
}

// Returns the latest used rdx.ID of current replica
func (cho *Chotki) Last() rdx.ID {
	cho.lastLock.Lock()
	defer cho.lastLock.Unlock()
	return cho.last
}

// nextLast atomically allocates the next local id, advancing the cached
// allocator state.
func (cho *Chotki) nextLast() rdx.ID {
	cho.lastLock.Lock()
	defer cho.lastLock.Unlock()
	cho.last = cho.last.IncPro(1).ZeroOff()
	return cho.last
}

// advanceLast bumps the cached allocator to id if it is behind. Called by drain
// only after the own-source ids up to id have been durably applied, so cho.last
// never runs ahead of the persisted version vector.
func (cho *Chotki) advanceLast(id rdx.ID) {
	cho.lastLock.Lock()
	defer cho.lastLock.Unlock()
	if cho.last.Less(id) {
		cho.last = id
	}
}

// Returns the write options of the Chotki instance.
func (cho *Chotki) WriteOptions() *pebble.WriteOptions {
	return cho.opts.PebbleWriteOptions
}

// Returns the logger of the Chotki instance.
func (cho *Chotki) Logger() utils.Logger {
	return cho.log
}

// Returns a new snapshot of the Chotki instance.
func (cho *Chotki) Snapshot() pebble.Reader {
	return cho.db.NewSnapshot()
}

// Returns the database of the Chotki instance.
func (cho *Chotki) Database() *pebble.DB {
	return cho.db
}

// Returns the directory of the Chotki instance.
func (cho *Chotki) Directory() string {
	return cho.dir
}

// Returns the new instance of the ORM object
func (cho *Chotki) ObjectMapper() *ORM {
	return NewORM(cho, cho.db.NewSnapshot())
}

// Starts listening on the given address.
func (cho *Chotki) Listen(addr string) error {
	return cho.net.Listen(addr)
}

// Stops listening on the given address.
func (cho *Chotki) Unlisten(addr string) error {
	return cho.net.Unlisten(addr)
}

// Connects to the given address.
func (cho *Chotki) Connect(addr string) error {
	return cho.net.Connect(addr)
}

// Connects to the given address pool.
func (cho *Chotki) ConnectPool(name string, addrs []string) error {
	return cho.net.ConnectPool(name, addrs)
}

// Disconnects from the given address.
func (cho *Chotki) Disconnect(addr string) error {
	return cho.net.Disconnect(addr)
}

// Returns the version vector of the Chotki instance as rdx.VV structure.
func (cho *Chotki) VersionVector() (vv rdx.VV, err error) {
	val, clo, err := cho.db.Get(host.VKey0)
	if err == nil {
		vv = make(rdx.VV)
		err = vv.PutTLV(val)
	}
	if clo != nil {
		_ = clo.Close()
	}
	return
}

func (cho *Chotki) AddHook(fid rdx.ID, hook Hook) {
	cho.lock.Lock()
	defer cho.lock.Unlock()

	list, _ := cho.hooks.LoadOrStore(fid, []Hook{})
	list = append(list, hook)
	cho.hooks.Store(fid, list)
}

func (cho *Chotki) RemoveHook(fid rdx.ID, hook Hook) (err error) {
	list, ok := cho.hooks.Load(fid)
	if !ok {
		return ErrHookNotFound
	}

	cho.lock.Lock()
	defer cho.lock.Unlock()

	new_list := make([]Hook, 0, len(list))
	for _, h := range list {
		if &h != &hook {
			new_list = append(new_list, h)
		}
	}
	if len(new_list) == len(list) {
		return ErrHookNotFound
	}
	cho.hooks.Store(fid, new_list)
	return
}

func (cho *Chotki) RemoveAllHooks(fid rdx.ID) {
	cho.hooks.Delete(fid)
}

// Broadcasts some records to all active replication sessions that this replica has, except one.
func (cho *Chotki) Broadcast(ctx context.Context, records protocol.Records, except string) {
	cho.outq.Range(func(name string, hose protocol.DrainCloser) bool {
		if name != except {
			EventsOutboundMetric.WithLabelValues(name).Add(float64(len(records)))
			if err := hose.Drain(ctx, records); err != nil {
				cho.log.ErrorCtx(ctx, "couldn't drain to hose", "err", err, "name", name)
				cho.outq.Delete(name)
			}
		}

		return true
	})
}

// Commits records to actual storage (pebble) and broadcasts the update to all active replication sessions.
// Increments replica last rdx.ID and stamps this update with it. This id will be used as an ID of a new object (if it is an object), also
// this will be the latest seen rdx.ID in the version vector.
// Uses an exclusive lock, so all commits are serialized, but remember that Drain/drain calls are not.
// All replication sessions will call Drain in parallel.
func (cho *Chotki) CommitPacket(ctx context.Context, lit byte, ref rdx.ID, body protocol.Records) (id rdx.ID, err error) {
	// prevent cancellation as it can make this function non atomic
	ctx = context.WithoutCancel(ctx)

	now := time.Now()
	defer func() {
		DrainTime.WithLabelValues("commit+broadcast").Observe(float64(time.Since(now)) / float64(time.Millisecond))
	}()
	cho.commitMutex.Lock()
	defer cho.commitMutex.Unlock()
	DrainTime.WithLabelValues("commit lock").Observe(float64(time.Since(now)) / float64(time.Millisecond))

	if cho.db == nil {
		return rdx.BadId, chotki_errors.ErrClosed
	}
	// allocate the next local id
	id = cho.nextLast()
	i := protocol.Record('I', id.ZipBytes())
	// this is typically some higher order structure (like for object it will be class id, for field update it will be object id)
	r := protocol.Record('R', ref.ZipBytes())
	packet := protocol.Record(lit, i, r, protocol.Join(body...))
	recs := protocol.Records{packet}
	// single packet = one object; no intra-batch cross-object uniqueness to check
	_, err = cho.drain(ctx, recs, nil)
	DrainTime.WithLabelValues("commit").Observe(float64(time.Since(now)) / float64(time.Millisecond))
	if err != nil {
		// Don't publish an id we failed to persist: a crash before the next
		// successful commit could reissue it while a peer already holds it.
		return
	}
	cho.Broadcast(ctx, recs, "")
	return
}

// CommitBatch applies edits as one all-or-nothing Pebble batch + broadcast. Each edit is stamped in
// slice order; correctness relies on 'E' ops being commutative Merge calls (no read-back).
func (cho *Chotki) CommitBatch(ctx context.Context, edits []host.Edit) (err error) {
	if len(edits) == 0 {
		return nil
	}
	// prevent cancellation as it can make this function non atomic
	ctx = context.WithoutCancel(ctx)

	now := time.Now()
	defer func() {
		DrainTime.WithLabelValues("commitbatch").Observe(float64(time.Since(now)) / float64(time.Millisecond))
	}()
	cho.commitMutex.Lock()
	defer cho.commitMutex.Unlock()

	if cho.db == nil {
		return chotki_errors.ErrClosed
	}

	recs := make(protocol.Records, 0, len(edits))
	for _, e := range edits {
		id := cho.nextLast() // one id per edit
		recs = append(recs, protocol.Record('E',
			protocol.Record('I', id.ZipBytes()),
			protocol.Record('R', e.Ref.ZipBytes()),
			protocol.Join(e.Body...)))
	}
	// drain applies the batch but doesn't broadcast; we do that below.
	// claims rejects two edits setting the same unique value on different
	// objects before the batch commits.
	claims := map[string]rdx.ID{}
	if _, err = cho.drain(ctx, recs, claims); err != nil {
		return err
	}
	cho.Broadcast(ctx, recs, "")
	return nil
}

// StartSequentialWrite acquires the cooperative read-modify-write bracket (see
// host.Host): hold it across both the read and commit of a state-dependent op
// (e.g. a Z-counter set). Commit methods don't take it. Not reentrant.
func (cho *Chotki) StartSequentialWrite() {
	cho.seqWriteMu.Lock()
}

// EndSequentialWrite releases the bracket taken by StartSequentialWrite.
func (cho *Chotki) EndSequentialWrite() {
	cho.seqWriteMu.Unlock()
}

type NetCollector struct {
	net               *network.Net
	read_buffers_size *prometheus.Desc
	write_batch_size  *prometheus.Desc
}

func NewNetCollector(net *network.Net) *NetCollector {
	return &NetCollector{
		net:               net,
		read_buffers_size: prometheus.NewDesc("chotki_net_read_buffer_size", "", []string{"peer"}, prometheus.Labels{}),
		write_batch_size:  prometheus.NewDesc("chotki_net_write_batch_size", "", []string{"peer"}, prometheus.Labels{}),
	}
}

func (n *NetCollector) Describe(d chan<- *prometheus.Desc) {
	d <- n.read_buffers_size
	d <- n.write_batch_size
}

func (n *NetCollector) Collect(m chan<- prometheus.Metric) {
	stats := n.net.GetStats()
	for name, v := range stats.ReadBuffers {
		m <- prometheus.MustNewConstMetric(n.read_buffers_size, prometheus.GaugeValue, float64(v), name)
	}
	for name, v := range stats.WriteBatches {
		m <- prometheus.MustNewConstMetric(n.write_batch_size, prometheus.GaugeValue, float64(v), name)
	}
}

type ChotkiCollector struct {
	chotki         *Chotki
	outq_size      *prometheus.Desc
	collected_prev map[string]struct{}
	lock           sync.Mutex
}

func NewChotkiCollector(chotki *Chotki) *ChotkiCollector {
	return &ChotkiCollector{
		chotki:    chotki,
		outq_size: prometheus.NewDesc("chotki_outq_len", "", []string{"name"}, prometheus.Labels{}),
	}
}

func (c *ChotkiCollector) Describe(d chan<- *prometheus.Desc) {
	d <- c.outq_size
}

func (n *ChotkiCollector) Collect(m chan<- prometheus.Metric) {
	n.lock.Lock()
	defer n.lock.Unlock()

	nw_collected := make(map[string]struct{})
	need_to_pass := make(map[string]bool)
	for key := range n.collected_prev {
		need_to_pass[key] = true
	}
	n.chotki.outq.Range(func(key string, value protocol.DrainCloser) bool {
		if q, ok := value.(*utils.FDQueue[protocol.Records]); ok {
			m <- prometheus.MustNewConstMetric(n.outq_size, prometheus.GaugeValue, float64(q.Size()), key)
			nw_collected[key] = struct{}{}
			need_to_pass[key] = false
		}
		return true
	})
	for name, v := range need_to_pass {
		if v { // we previously set this, but now it was deleted
			m <- prometheus.MustNewConstMetric(n.outq_size, prometheus.GaugeValue, 0, name)
		}
	}
	n.collected_prev = nw_collected
}

// Returns a list of prometheus collectors for the Chotki instance.
func (cho *Chotki) Metrics() []prometheus.Collector {
	cho.db.Metrics()
	return []prometheus.Collector{
		EventsMetric,
		EventsOutboundMetric,
		NewNetCollector(cho.net),
		EventsBatchSize,
		NewPebbleCollector(cho.db),
		NewChotkiCollector(cho),
		replication.OpenedIterators,
		replication.OpenedSnapshots,
		replication.SessionsStates,
		DrainTime,
		indexes.ReindexTaskCount,
		indexes.ReindexResults,
		indexes.ReindexDuration,
		indexes.ReindexCount,
		indexes.ReindexTaskStates,
		DiffSyncSize,
	}
}

// Handles all updates and writes them to storage; types 'C','O','E','H','D',
// 'V','B','P','Y' (not RDX types). Drop-on-error: the Y/C/O/E records apply as
// one all-or-nothing batch, so applied is len(recs) on success, 0 on error, and
// replication relays only a fully-persisted batch. claims (CommitBatch only)
// rejects intra-batch duplicate unique values.
func (cho *Chotki) drain(ctx context.Context, recs protocol.Records, claims map[string]rdx.ID) (applied int, err error) {
	EventsMetric.Add(float64(len(recs)))
	var calls []CallHook
	// One batch for the non-sync packets (Y/C/O/E), applied once after the loop.
	// Sync packets (H/D/V) use their own per-sync batch, committed on 'V'.
	pb := pebble.Batch{}
	classChanged := false
	var created []rdx.ID     // objects created here; reindexed post-commit
	var syncCreated []rdx.ID // objects created by diff-sync 'V's; reindexed post-flush
	// Highest own-source id; cho.last advances to it only after pb is durable
	// (in flush), never in the loop, or a crash could reissue an id.
	var maxOwn rdx.ID
	// flush persists pb, then advances the allocator. Deferred so it runs on
	// every exit path. On error pb (an in-memory buffer) is dropped and applied
	// zeroed: nothing relayed, allocator unchanged.
	flushed := false
	flush := func() {
		if flushed {
			return
		}
		flushed = true
		if err != nil {
			applied = 0
			return
		}
		if pb.Count() > 0 {
			if ae := cho.db.Apply(&pb, cho.opts.PebbleWriteOptions); ae != nil {
				err = ae
				applied = 0
				return
			}
		}
		if maxOwn != rdx.ID0 {
			cho.advanceLast(maxOwn)
		}
	}
	defer flush()
	for _, packet := range recs {
		if err != nil {
			break
		}

		lit, id, ref, body, parseErr := replication.ParsePacket(packet)
		if parseErr != nil {
			cho.log.WarnCtx(ctx, "bad packet", "err", parseErr)
			return applied, parseErr
		}

		// An own-src record must advance the allocator or future commits reuse
		// its seq. Sources: local commits, or a session draining our own history
		// back (e.g. after a restore). cho.last advances after the durable apply.
		if id.Src() == cho.src && maxOwn.Less(id) {
			if id.Off() != 0 {
				return applied, rdx.ErrBadPacket
			}
			maxOwn = id
		}

		cho.log.DebugCtx(ctx, "new packet", "type", string(lit), "packet", id.String())

		switch lit {
		case 'Y': // creates a replica log
			if ref != rdx.ID0 {
				return applied, ErrBadYPacket
			}
			err = cho.ApplyOY('Y', id, ref, body, &pb, claims)

		case 'C': // creates a class
			err = cho.ApplyC(id, ref, body, &pb, &calls)
			if err == nil {
				// clear the type cache after the batch applies, so a concurrent
				// rebuild can't repopulate it from the pre-class state.
				classChanged = true
			}

		case 'O': // creates an object
			if ref == rdx.ID0 {
				return applied, ErrBadOPacket
			}
			err = cho.ApplyOY('O', id, ref, body, &pb, claims)
			if err == nil {
				created = append(created, id)
			}

		case 'E': // edits an object
			if ref == rdx.ID0 {
				return applied, ErrBadEPacket
			}
			// created also collects edit targets whose class was unresolvable
			// mid-batch (object's 'O' not visible yet); reindexed post-commit.
			err = cho.ApplyE(id, ref, body, &pb, &calls, &created, claims)

		case 'H': // handshake
			d := cho.db.NewBatch()
			// One diff sync per src: a new handshake displaces the origin's
			// stale sync points (keyed by snapshot id, so match on id.Src()).
			incomingVia := replication.SessionIdFromCtx(ctx)
			activeSyncs := make([]rdx.ID, 0)
			cho.syncs.Range(func(key rdx.ID, value *syncPoint) bool {
				// Displace only on a genuine supersede: same session (restart) or
				// strictly older snapshot. Evicting a live diff via another session
				// would flap a cyclic topology; a tree is assumed (see README).
				if key.Src() == id.Src() && (value.via == incomingVia || key.Less(id)) {
					activeSyncs = append(activeSyncs, key)
				}
				return true
			})
			for _, s := range activeSyncs {
				if old, ok := cho.syncs.Load(s); ok {
					old.close()
				}
				cho.syncs.Delete(s)
				cho.log.InfoCtx(ctx, "deleted active sync", "id", s.String())
			}
			err = cho.ApplyH(id, ref, body, d)
			if err == nil {
				// Store after ApplyH so no goroutine can Load the sync point
				// until the batch is fully initialized.
				cho.syncs.Store(id, &syncPoint{
					batch: d,
					start: time.Now(),
					via:   replication.SessionIdFromCtx(ctx),
				})
				cho.log.InfoCtx(ctx, "created new sync point", "id", id.String())
			} else {
				_ = d.Close()
			}

		case 'D': // diff packet
			// load sync point if exists
			s, ok := cho.syncs.Load(id)
			if !ok {
				return applied, ErrSyncUnknown
			}
			s.mu.Lock()
			if s.batch == nil {
				// Sync point exists in the map but its batch has already been
				// closed (timeout in cleanSyncs, session end, displaced by a
				// new 'H', or a concurrent 'V'). Silently skipping would cause
				// partial data loss invisible to the peer, so surface the same
				// error as for a missing sync point and let the session restart.
				s.mu.Unlock()
				return applied, ErrSyncUnknown
			}
			err = cho.ApplyD(id, ref, body, s.batch, &s.created, claims)
			s.mu.Unlock()
			// applied into the sync point's own batch, not pb

		case 'V': // version vector sent in the end of diff sync
			// load sync point if exists
			s, ok := cho.syncs.Load(id)
			if !ok {
				return applied, ErrSyncUnknown
			}
			s.mu.Lock()
			if s.batch == nil {
				// See the 'D' case for rationale: batch was closed out from
				// under us, so treat it the same as an unknown sync.
				s.mu.Unlock()
				return applied, ErrSyncUnknown
			}
			DiffSyncSize.Observe(float64(s.batch.Len()))
			err = cho.ApplyV(id, ref, body, s.batch)
			if err == nil {
				// durability point for everything the diff sync delivered
				err = cho.db.Apply(s.batch, cho.opts.PebbleWriteOptions)
				// db.Apply doesn't return the batch to the pool; close it.
				_ = s.batch.Close()
				s.batch = nil
				// finished either way; a failed Apply restarts the diff sync.
				cho.syncs.Delete(id)
			}
			if err == nil {
				// gated on the Apply persisting — a rejected batch wrote nothing.
				//
				// A diff-sync can carry our own history back (e.g. after a
				// restore), advancing the persisted VV but not the allocator;
				// advance it from the merged VV so no id is reissued.
				if vv, verr := cho.VersionVector(); verr == nil {
					cho.advanceLast(vv.GetID(cho.src))
				} else {
					cho.log.WarnCtx(ctx, "could not read VV to advance allocator after sync", "err", verr)
				}
				// A diff-sync may carry now-durable class changes; drop the type
				// cache unconditionally (infrequent, cheap to rebuild).
				cho.types.Clear()
				// A class in pb isn't durable until flush(), so reindex these
				// post-flush alongside `created`.
				syncCreated = append(syncCreated, s.created...)
				cho.log.InfoCtx(ctx, "applied diff batch and deleted it", "id", id)
			}
			s.mu.Unlock()

		case 'B': // session end
			cho.log.InfoCtx(ctx, "received session end", "id", id.String(), "data", string(body))
			// close and delete sync point if not already
			if s, ok := cho.syncs.Load(id); ok {
				s.close()
			}
			cho.syncs.Delete(id)
		case 'P': // ping noop
		default:
			return applied, fmt.Errorf("unsupported packet type %c", lit)
		}

		if err == nil {
			applied++
		}
	}

	// Persist pb and advance the allocator before running hooks that read the
	// merged state. flush is idempotent; the deferred call above is now a no-op.
	flush()
	if err != nil {
		return
	}
	if classChanged {
		cho.types.Clear()
	}

	// Reindex created objects from their committed values, so an edit that
	// arrived before its create is picked up now. Dedup by id: ApplyD/ApplyE
	// append one entry per object and per deferred field.
	if len(created)+len(syncCreated) > 0 { // the common no-creates drain allocates nothing
		syncCreated = append(syncCreated, created...)
		seen := make(map[rdx.ID]struct{}, len(syncCreated))
		for _, oid := range syncCreated {
			key := oid.ZeroOff()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			if e := cho.IndexManager.IndexObject(key); e != nil {
				cho.log.WarnCtx(ctx, "post-commit index failed", "oid", key.String(), "err", e)
			}
		}
	}

	if len(calls) > 0 {
		for _, call := range calls {
			go call.hook(cho, call.id)
		}
	}

	return
}

// Public for drain method with some additional metrics
func (cho *Chotki) Drain(ctx context.Context, recs protocol.Records) (err error) {
	_, err = cho.DrainApplied(ctx, recs)
	return
}

// AbortSyncsVia closes pending diff-sync points created by the given session.
// Its 'D'/'V' packets travel through that connection, so once it's gone the
// batch can't complete; a 'V' relayed later would apply the handshake VV
// without the data that died with it — silent data loss (see tla/).
func (cho *Chotki) AbortSyncsVia(ctx context.Context, sessionId string) {
	if sessionId == "" {
		return
	}
	cho.syncs.Range(func(key rdx.ID, s *syncPoint) bool {
		if s.via == sessionId {
			s.close()
			cho.syncs.Delete(key)
			cho.log.InfoCtx(ctx, "aborted sync point of a closed session", "id", key.String())
		}
		return true
	})
}

// DrainApplied is Drain plus the applied-record count. Draining is
// all-or-nothing, so the count is len(recs) on success and 0 on error.
func (cho *Chotki) DrainApplied(ctx context.Context, recs protocol.Records) (applied int, err error) {
	now := time.Now()
	defer func() {
		DrainTime.WithLabelValues("drain").Observe(float64(time.Since(now)) / float64(time.Millisecond))
	}()
	cho.lock.RLock()
	defer cho.lock.RUnlock()
	if cho.db == nil {
		return 0, chotki_errors.ErrClosed
	}
	EventsBatchSize.Observe(float64(len(recs)))
	return cho.drain(ctx, recs, nil)
}

func dumpKVString(key, value []byte) (str string) {
	if len(key) == host.LidLKeyLen {
		id, rdt := host.OKeyIdRdt(key)
		str = fmt.Sprintf("%s.%c:\t%s", id, rdt, rdx.Xstring(rdt, value))
	}
	return
}

// Dumps all objects to the writer.
func (cho *Chotki) DumpObjects(writer io.Writer) {
	io := pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	}
	i, err := cho.db.NewIter(&io)
	if err != nil {
		return
	}
	defer i.Close()
	for i.SeekGE([]byte{'O'}); i.Valid(); i.Next() {
		fmt.Fprintln(writer, dumpKVString(i.Key(), i.Value()))
	}
}

// Dumps all version vectors to the writer.
func (cho *Chotki) DumpVV(writer io.Writer) {
	io := pebble.IterOptions{
		LowerBound: []byte{'V'},
		UpperBound: []byte{'W'},
	}
	i, err := cho.db.NewIter(&io)
	if err != nil {
		return
	}
	defer i.Close()
	for i.SeekGE(host.VKey0); i.Valid(); i.Next() {
		id := rdx.IDFromBytes(i.Key()[1:])
		vv := make(rdx.VV)
		_ = vv.PutTLV(i.Value())
		fmt.Fprintln(writer, id.String(), " -> ", vv.String())
	}
}

// Dumps all objects and version vectors to the writer.
func (cho *Chotki) DumpAll(writer io.Writer) {
	cho.DumpObjects(writer)
	fmt.Fprintln(writer, "")
	cho.DumpVV(writer)
}
