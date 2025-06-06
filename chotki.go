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
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
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
	ErrBadHPacket     = errors.New("chotki: bad handshake packet")
	ErrBadEPacket     = errors.New("chotki: bad E packet")
	ErrBadVPacket     = errors.New("chotki: bad V packet")
	ErrBadYPacket     = errors.New("chotki: bad Y packet")
	ErrBadLPacket     = errors.New("chotki: bad L packet")
	ErrBadTPacket     = errors.New("chotki: bad T packet")
	ErrBadOPacket     = errors.New("chotki: bad O packet")
	ErrSrcUnknown     = errors.New("chotki: source unknown")
	ErrSyncUnknown    = errors.New("chotki: sync session unknown")
	ErrBadRRecord     = errors.New("chotki: bad ref record")
	ErrClosed         = errors.New("chotki: no replica open")

	ErrBadTypeDescription  = errors.New("chotki: bad type description")
	ErrObjectUnknown       = errors.New("chotki: unknown object")
	ErrTypeUnknown         = errors.New("chotki: unknown object type")
	ErrUnknownFieldInAType = errors.New("chotki: unknown field for the type")
	ErrBadClass            = errors.New("chotki: bad class description")

	ErrOutOfOrder      = errors.New("chotki: order fail: sequence gap")
	ErrCausalityBroken = errors.New("chotki: order fail: refs an unknown op")

	ErrFullscanIndexField                 = errors.New("chotki: field can't have fullscan index")
	ErrHashIndexFieldNotFirst             = errors.New("chotki: field can't have hash index if type is not FIRST")
	ErrHashIndexUinqueConstraintViolation = errors.New("chotki: hash index unique constraint violation")
)

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

	o.Merger = &pebble.Merger{
		Name: "CRDT",
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			target := make([]byte, len(value))
			copy(target, value)
			var rdt byte
			switch key[0] {
			case 'O':
				_, rdt = OKeyIdRdt(key)
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
	batch *pebble.Batch
	start time.Time
}

// TLV all the way down
type Chotki struct {
	last      rdx.ID
	src       uint64
	clock     rdx.Clock
	cancelCtx context.CancelFunc
	waitGroup *sync.WaitGroup

	lock         sync.RWMutex
	commitMutex  sync.Mutex
	db           *pebble.DB
	net          *protocol.Net
	dir          string
	opts         Options
	log          utils.Logger
	counterCache sync.Map
	indexManager *IndexManager

	outq  *xsync.MapOf[string, protocol.DrainCloser] // queues to broadcast all new packets
	syncs *xsync.MapOf[rdx.ID, *syncPoint]
	hooks *xsync.MapOf[rdx.ID, []Hook]
	types *xsync.MapOf[rdx.ID, Fields]
}

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

func (cho *Chotki) cleanSyncs(ctx context.Context) {
	for ctx.Err() == nil {
		cho.syncs.Range(func(id rdx.ID, s *syncPoint) bool {
			if time.Since(s.start) > cho.opts.MaxSyncDuration {
				s.batch.Close()
				cho.syncs.Delete(id)
				cho.log.WarnCtx(ctx, "diff sync took too long", "id", id, "duration", time.Since(s.start))
			}
			return true
		})
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
		}
	}
}

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
		db:    db,
		src:   opts.Src,
		dir:   absdir,
		log:   opts.Logger,
		opts:  opts,
		clock: &rdx.LocalLogicalClock{Source: opts.Src},

		outq:      xsync.NewMapOf[string, protocol.DrainCloser](),
		syncs:     xsync.NewMapOf[rdx.ID, *syncPoint](),
		hooks:     xsync.NewMapOf[rdx.ID, []Hook](),
		types:     xsync.NewMapOf[rdx.ID, Fields](),
		cancelCtx: cancel,
		waitGroup: &wg,
	}

	cho.net = protocol.NewNet(cho.log,
		func(name string) protocol.FeedDrainCloserTraced { // new connection

			queue := utils.NewFDQueue[protocol.Records](cho.opts.BroadcastQueueMaxSize, cho.opts.BroadcastQueueTimeLimit, cho.opts.BroadcastQueueMinBatchSize)
			if q, loaded := cho.outq.LoadAndStore(name, queue); loaded && q != nil {
				cho.log.Warn(fmt.Sprintf("closing the old conn to %s", name))
				if err := q.Close(); err != nil {
					cho.log.Error(fmt.Sprintf("couldn't close conn %s", name), "err", err)
				}
			}

			return &Syncer{
				Src:        cho.src,
				Host:       &cho,
				Mode:       SyncRWLive,
				PingPeriod: cho.opts.PingPeriod,
				PingWait:   cho.opts.PingWait,
				Name:       name,
				log:        cho.log,
				oqueue:     queue,
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
		&protocol.NetTlsConfigOpt{Config: opts.TlsConfig},
		&protocol.NetReadBatchOpt{
			ReadAccumTimeLimit: cho.opts.ReadAccumTimeLimit,
			BufferMaxSize:      cho.opts.ReadMaxBufferSize,
			BufferMinToProcess: cho.opts.ReadMinBufferSizeToProcess,
		},
		&protocol.TcpBufferSizeOpt{Read: cho.opts.TcpReadBufferSize, Write: cho.opts.TcpWriteBufferSize},
		&protocol.NetWriteTimeoutOpt{Timeout: cho.opts.WriteTimeout},
	)
	cho.indexManager = newIndexManager(&cho)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cho.indexManager.CheckReindexTasks(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		cho.cleanSyncs(ctx)
	}()

	if !exists {
		id0 := rdx.IDFromSrcSeqOff(opts.Src, 0, 0)

		init := append(protocol.Records(nil), Log0...)
		init = append(init, opts.Log1...)
		init = append(init, protocol.Record('Y',
			protocol.Record('I', id0.ZipBytes()),
			protocol.Record('R', rdx.ID0.ZipBytes()),
			protocol.Record('S', rdx.Stlv(opts.Name)),
			protocol.Record('V', []byte{}),
			protocol.Record('S', rdx.Stlv("")),
		))

		if err = cho.drain(context.Background(), init); err != nil {
			return nil, errors.Join(err, fmt.Errorf("unable to drain initial data to chotki"))
		}
	}

	vv, err := cho.VersionVector()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("unable to get version vector"), err)
	}

	cho.last = vv.GetID(cho.src)

	return &cho, nil
}

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
	if cho.db != nil {
		if err := cho.db.Close(); err != nil {
			cho.log.Error("couldn't close Pebble", "err", err)
		}

		cho.db = nil
	}

	cho.outq.Clear()
	cho.syncs.Clear()
	cho.hooks.Clear()
	cho.types.Clear()

	cho.src = 0
	cho.last = rdx.ID0

	return nil
}

func (cho *Chotki) Counter(rid rdx.ID, offset uint64, updatePeriod time.Duration) *AtomicCounter {
	counter, _ := cho.counterCache.LoadOrStore(rid.ToOff(offset), NewAtomicCounter(cho, rid, offset, updatePeriod))
	return counter.(*AtomicCounter)
}

func (cho *Chotki) KeepAliveLoop() {
	var err error
	for err == nil {
		time.Sleep(time.Second * 30)
		err = cho.KeepAlive()
	}
	if err != ErrClosed {
		cho.log.Error(err.Error())
		cho.log.Error("keep alives stop")
	}
}

func (cho *Chotki) KeepAlive() error {
	oid := rdx.IDfromSrcPro(cho.src, 0)
	oldtlv, err := cho.ObjectRDTFieldTLV(oid.ToOff(YAckOff), 'V')
	if err != nil {
		return err
	}
	mysrc := cho.src
	newvv, err := cho.VersionVector()
	if err != nil {
		return err
	}
	oldvv := make(rdx.VV)
	_ = oldvv.PutTLV(oldtlv)
	delete(oldvv, mysrc)
	delete(newvv, mysrc)
	tlv_delta := rdx.VVdelta(oldvv, newvv)
	if len(tlv_delta) == 0 {
		return nil
	}
	d := protocol.Records{
		protocol.Record('F', rdx.ZipUint64(2)),
		protocol.Record('V', tlv_delta),
	}
	_, err = cho.CommitPacket(context.Background(), 'E', oid, d)
	return err
}

// ToyKV convention key, lit O, then O00000-00000000-000 id
func (cho *Chotki) Source() uint64 {
	return cho.src
}

func (cho *Chotki) Clock() rdx.Clock {
	return cho.clock
}

func (cho *Chotki) Last() rdx.ID {
	return cho.last
}

func (cho *Chotki) Snapshot() pebble.Reader {
	return cho.db.NewSnapshot()
}

func (cho *Chotki) Database() *pebble.DB {
	return cho.db
}

func (cho *Chotki) Directory() string {
	return cho.dir
}

func (cho *Chotki) ObjectMapper() *ORM {
	return NewORM(cho, cho.db.NewSnapshot())
}

func (cho *Chotki) RestoreNet() error {
	i := cho.db.NewIter(&pebble.IterOptions{})
	defer i.Close()

	for i.SeekGE([]byte{'l'}); i.Valid() && i.Key()[0] == 'L'; i.Next() {
		address := string(i.Key()[1:])
		_ = cho.net.Listen(address)
	}

	for i.SeekGE([]byte{'c'}); i.Valid() && i.Key()[0] == 'C'; i.Next() {
		address := string(i.Key()[1:])
		_ = cho.net.Connect(address)
	}

	return nil
}

func (cho *Chotki) Listen(addr string) error {
	return cho.net.Listen(addr)
}

func (cho *Chotki) Unlisten(addr string) error {
	return cho.net.Unlisten(addr)
}

func (cho *Chotki) Connect(addr string) error {
	return cho.net.Connect(addr)
}

func (cho *Chotki) ConnectPool(name string, addrs []string) error {
	return cho.net.ConnectPool(name, addrs)
}

func (cho *Chotki) Disconnect(addr string) error {
	return cho.net.Disconnect(addr)
}

func (cho *Chotki) VersionVector() (vv rdx.VV, err error) {
	val, clo, err := cho.db.Get(VKey0)
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

// Here new packets are timestamped and queued for save
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
		return rdx.BadId, ErrClosed
	}
	id = cho.last.IncPro(1).ZeroOff()
	i := protocol.Record('I', id.ZipBytes())
	r := protocol.Record('R', ref.ZipBytes())
	packet := protocol.Record(lit, i, r, protocol.Join(body...))
	recs := protocol.Records{packet}
	err = cho.drain(ctx, recs)
	DrainTime.WithLabelValues("commit").Observe(float64(time.Since(now)) / float64(time.Millisecond))
	cho.Broadcast(ctx, recs, "")
	return
}

type NetCollector struct {
	net               *protocol.Net
	read_buffers_size *prometheus.Desc
	write_batch_size  *prometheus.Desc
}

func NewNetCollector(net *protocol.Net) *NetCollector {
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
func (cho *Chotki) Metrics() []prometheus.Collector {
	cho.db.Metrics()
	return []prometheus.Collector{
		EventsMetric,
		EventsOutboundMetric,
		NewNetCollector(cho.net),
		EventsBatchSize,
		NewPebbleCollector(cho.db),
		NewChotkiCollector(cho),
		OpenedIterators,
		OpenedSnapshots,
		SessionsStates,
		DrainTime,
		ReindexTaskCount,
		ReindexResults,
		ReindexDuration,
		ReindexCount,
		ReindexTaskStates,
	}
}

func (cho *Chotki) drain(ctx context.Context, recs protocol.Records) (err error) {
	EventsMetric.Add(float64(len(recs)))
	var calls []CallHook
	for _, packet := range recs { // parse the packets
		if err != nil {
			break
		}

		lit, id, ref, body, parseErr := ParsePacket(packet)
		if parseErr != nil {
			cho.log.WarnCtx(ctx, "bad packet", "err", parseErr)
			return parseErr
		}

		if id.Src() == cho.src && cho.last.Less(id) {
			if id.Off() != 0 {
				return rdx.ErrBadPacket
			}
			cho.last = id
		}

		pb, noApply := pebble.Batch{}, false

		cho.log.DebugCtx(ctx, "new packet", "type", string(lit), "packet", id.String())

		switch lit {
		case 'Y': // create replica log
			if ref != rdx.ID0 {
				return ErrBadYPacket
			}
			err = cho.ApplyOY('Y', id, ref, body, &pb)

		case 'C': // create class
			err = cho.ApplyC(id, ref, body, &pb, &calls)
			if err == nil {
				// clear cache for classes if class changed
				cho.types.Clear()
			}

		case 'O': // create object
			if ref == rdx.ID0 {
				return ErrBadOPacket
			}
			err = cho.ApplyOY('O', id, ref, body, &pb)

		case 'E': // edit object
			if ref == rdx.ID0 {
				return ErrBadEPacket
			}
			err = cho.ApplyE(id, ref, body, &pb, &calls)

		case 'H': // handshake
			d := cho.db.NewBatch()
			cho.syncs.Store(id, &syncPoint{batch: d, start: time.Now()})
			err = cho.ApplyH(id, ref, body, d)

		case 'D': // diff
			s, ok := cho.syncs.Load(id)
			if !ok {
				return ErrSyncUnknown
			}
			err = cho.ApplyD(id, ref, body, s.batch)
			noApply = true

		case 'V':
			s, ok := cho.syncs.Load(id)
			if !ok {
				return ErrSyncUnknown
			}
			err = cho.ApplyV(id, ref, body, s.batch)
			if err == nil {
				err = cho.db.Apply(s.batch, cho.opts.PebbleWriteOptions)
				cho.syncs.Delete(id)
				cho.log.InfoCtx(ctx, "applied diff batch and deleted it", "id", id)
			}
			noApply = true

		case 'B': // bye dear
			cho.log.InfoCtx(ctx, "received session end", "id", id.String(), "data", string(body))
			cho.syncs.Delete(id)
		case 'P': // ping noop
		default:
			return fmt.Errorf("unsupported packet type %c", lit)
		}

		if !noApply && err == nil {
			if err := cho.db.Apply(&pb, cho.opts.PebbleWriteOptions); err != nil {
				return err
			}
		}
	}

	if err != nil { // fixme separate packets
		return
	}

	if len(calls) > 0 {
		for _, call := range calls {
			go call.hook(cho, call.id)
		}
	}

	return
}

func (cho *Chotki) Drain(ctx context.Context, recs protocol.Records) (err error) {
	now := time.Now()
	defer func() {
		DrainTime.WithLabelValues("drain").Observe(float64(time.Since(now)) / float64(time.Millisecond))
	}()
	cho.lock.RLock()
	defer cho.lock.RUnlock()
	if cho.db == nil {
		return ErrClosed
	}
	EventsBatchSize.Observe(float64(len(recs)))
	return cho.drain(ctx, recs)
}

func dumpKVString(key, value []byte) (str string) {
	if len(key) == LidLKeyLen {
		id, rdt := OKeyIdRdt(key)
		str = fmt.Sprintf("%s.%c:\t%s", id, rdt, rdx.Xstring(rdt, value))
	}
	return
}

func (cho *Chotki) DumpObjects(writer io.Writer) {
	io := pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	}
	i := cho.db.NewIter(&io)
	defer i.Close()
	for i.SeekGE([]byte{'O'}); i.Valid(); i.Next() {
		fmt.Fprintln(writer, dumpKVString(i.Key(), i.Value()))
	}
}

func (cho *Chotki) DumpVV(writer io.Writer) {
	io := pebble.IterOptions{
		LowerBound: []byte{'V'},
		UpperBound: []byte{'W'},
	}
	i := cho.db.NewIter(&io)
	defer i.Close()
	for i.SeekGE(VKey0); i.Valid(); i.Next() {
		id := rdx.IDFromBytes(i.Key()[1:])
		vv := make(rdx.VV)
		_ = vv.PutTLV(i.Value())
		fmt.Fprintln(writer, id.String(), " -> ", vv.String())
	}
}

func (cho *Chotki) DumpAll(writer io.Writer) {
	cho.DumpObjects(writer)
	fmt.Fprintln(writer, "")
	cho.DumpVV(writer)
}
