package chotki

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/utils"
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
	ErrBadValueForAType    = errors.New("chotki: bad value for the type")
	ErrBadClass            = errors.New("chotki: bad class description")

	ErrOutOfOrder      = errors.New("chotki: order fail: sequence gap")
	ErrCausalityBroken = errors.New("chotki: order fail: refs an unknown op")
)

var pebbleWriteOptions = pebble.WriteOptions{Sync: true}

type Options struct {
	pebble.Options

	Src          uint64
	Name         string
	Log1         protocol.Records
	MaxLogLen    int64
	RelaxedOrder bool
	Logger       utils.Logger

	TlsConfig *tls.Config
}

func (o *Options) SetDefaults() {
	if o.MaxLogLen == 0 {
		o.MaxLogLen = 1 << 23
	}

	o.Merger = &pebble.Merger{
		Name: "CRDT",
		Merge: func(key, value []byte) (pebble.ValueMerger, error) {
			/*if len(key) != 10 {
				return nil, nil
			}*/
			id, rdt := OKeyIdRdt(key)
			pma := PebbleMergeAdaptor{
				id:   id,
				rdt:  rdt,
				vals: [][]byte{value},
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

// TLV all the way down
type Chotki struct {
	last  rdx.ID
	src   uint64
	clock rdx.Clock

	lock sync.Mutex
	db   *pebble.DB
	orm  *ORM
	net  *protocol.Net
	dir  string
	opts Options
	log  utils.Logger

	outq  *xsync.MapOf[string, protocol.DrainCloser] // queues to broadcast all new packets
	syncs *xsync.MapOf[rdx.ID, *pebble.Batch]
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

func Open(dirname string, opts Options) (*Chotki, error) {
	exists, err := Exists(dirname)
	if err != nil {
		return nil, err
	}

	opts.SetDefaults() // todo param

	db, err := pebble.Open(dirname, &opts.Options)
	if err != nil {
		return nil, err
	}

	cho := Chotki{
		db:   db,
		src:  opts.Src,
		dir:  dirname,
		log:  opts.Logger,
		opts: opts,

		clock: &rdx.LocalLogicalClock{Source: opts.Src},

		outq:  xsync.NewMapOf[string, protocol.DrainCloser](),
		syncs: xsync.NewMapOf[rdx.ID, *pebble.Batch](),
		hooks: xsync.NewMapOf[rdx.ID, []Hook](),
		types: xsync.NewMapOf[rdx.ID, Fields](),
	}

	cho.net = protocol.NewNet(cho.log, func(conn net.Conn) protocol.FeedDrainCloser {
		return &Syncer{
			Host: &cho,
			Mode: SyncRWLive,
			Name: conn.RemoteAddr().String(),
			Log:  cho.log,
		}
	})
	cho.net.TlsConfig = opts.TlsConfig

	cho.orm = NewORM(&cho, db.NewSnapshot())

	if !exists {
		id0 := rdx.IDFromSrcSeqOff(opts.Src, 0, 0)

		init := append(protocol.Records(nil), Log0...)
		init = append(init, opts.Log1...)
		init = append(init, protocol.Record('Y',
			protocol.Record('I', id0.ZipBytes()),
			protocol.Record('R', rdx.ID0.ZipBytes()),
			protocol.Record('S', rdx.Stlv(opts.Name)),
		))

		if err = cho.Drain(init); err != nil {
			return nil, err
		}
	}

	vv, err := cho.VersionVector()
	if err != nil {
		return nil, err
	}

	cho.last = vv.GetID(cho.src)

	return &cho, nil
}

func (cho *Chotki) Close() error {
	cho.lock.Lock()
	defer cho.lock.Unlock()

	if cho.net != nil {
		if err := cho.net.Close(); err != nil {
			cho.log.Error("couldn't close network", "err", err)
		}

		cho.net = nil
	}

	if cho.orm != nil {
		if err := cho.orm.Close(); err != nil {
			cho.log.Error("couldn't close ORM", "err", err)
		}

		cho.orm = nil
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

func (cho *Chotki) ObjectMapper() *ORM {
	return cho.orm
}

func (cho *Chotki) RestoreNet(ctx context.Context) error {
	i := cho.db.NewIter(&pebble.IterOptions{})
	defer i.Close()

	for i.SeekGE([]byte{'l'}); i.Valid() && i.Key()[0] == 'L'; i.Next() {
		address := string(i.Key()[1:])
		_ = cho.net.Listen(ctx, address)
	}

	for i.SeekGE([]byte{'c'}); i.Valid() && i.Key()[0] == 'C'; i.Next() {
		address := string(i.Key()[1:])
		_ = cho.net.Connect(ctx, address)
	}

	return nil
}

func (cho *Chotki) Listen(ctx context.Context, addr string) error {
	return cho.net.Listen(ctx, addr)
}

func (cho *Chotki) Unlisten(addr string) error {
	return cho.net.Unlisten(addr)
}

func (cho *Chotki) Connect(ctx context.Context, addr string) error {
	return cho.net.Connect(ctx, addr)
}

func (cho *Chotki) Disconnect(addr string) error {
	return cho.net.Disconnect(addr)
}

func (cho *Chotki) VersionVector() (vv rdx.VV, err error) {
	key0 := VKey(rdx.ID0)
	val, clo, err := cho.db.Get(key0)
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

func (cho *Chotki) AddPacketHose(name string) (feed protocol.FeedCloser) {
	if q, deleted := cho.outq.LoadAndDelete(name); deleted && q != nil {
		cho.log.Warn(fmt.Sprintf("closing the old conn to %s", name))
		if err := q.Close(); err != nil {
			cho.log.Error(fmt.Sprintf("couldn't close conn %s", name), "err", err)
		}
	}

	queue := utils.NewFDQueue[protocol.Records](SyncOutQueueLimit, time.Millisecond)
	cho.outq.Store(name, queue)
	return queue
}

func (cho *Chotki) RemovePacketHose(name string) error {
	if q, deleted := cho.outq.LoadAndDelete(name); deleted && q != nil {
		cho.log.Warn(fmt.Sprintf("closing the old conn to %s", name))
		if err := q.Close(); err != nil {
			cho.log.Error(fmt.Sprintf("couldn't close conn %s", name), "err", err)
		}
	}
	return nil
}

func (cho *Chotki) BroadcastPacket(records protocol.Records, except string) {
	cho.outq.Range(func(name string, hose protocol.DrainCloser) bool {
		if name != except {
			if err := hose.Drain(records); err != nil {
				cho.outq.Delete(name)
			} else {
				cho.log.Error("couldn't drain to hose", "err", err)
			}
		}

		return true
	})
}

// Here new packets are timestamped and queued for save
func (cho *Chotki) CommitPacket(lit byte, ref rdx.ID, body protocol.Records) (id rdx.ID, err error) {
	cho.lock.Lock()
	defer cho.lock.Unlock()

	if cho.db == nil {
		return rdx.BadId, ErrClosed
	}
	id = (cho.last & ^rdx.OffMask) + rdx.ProInc
	i := protocol.Record('I', id.ZipBytes())
	r := protocol.Record('R', ref.ZipBytes())
	packet := protocol.Record(lit, i, r, protocol.Join(body...))
	recs := protocol.Records{packet}
	err = cho.Drain(recs)
	cho.BroadcastPacket(recs, "")
	return
}

func (cho *Chotki) Drain(recs protocol.Records) (err error) {
	var calls []CallHook

	for _, packet := range recs { // parse the packets
		if err != nil {
			break
		}

		lit, id, ref, body, parseErr := ParsePacket(packet)
		if parseErr != nil {
			cho.log.Warn("bad packet", "err", parseErr)
			return parseErr
		}

		if id.Src() == cho.src && id > cho.last {
			if id.Off() != 0 {
				return rdx.ErrBadPacket
			}
			cho.last = id
		}

		pb, noApply := pebble.Batch{}, false

		switch lit {
		case 'Y': // create replica log
			cho.log.Debug("'Y' sync session", "id", id.String())
			if ref != rdx.ID0 {
				return ErrBadYPacket
			}
			err = cho.ApplyOY('Y', id, ref, body, &pb)

		case 'C': // create class
			cho.log.Debug("'C' sync session", "id", id.String())
			err = cho.ApplyC(id, ref, body, &pb)

		case 'O': // create object
			cho.log.Debug("'O' sync session", "id", id.String())
			if ref == rdx.ID0 {
				return ErrBadOPacket
			}
			err = cho.ApplyOY('O', id, ref, body, &pb)

		case 'E': // edit object
			cho.log.Debug("'E' sync session", "id", id.String())
			if ref == rdx.ID0 {
				return ErrBadEPacket
			}
			err = cho.ApplyE(id, ref, body, &pb, &calls)

		case 'H': // handshake
			cho.log.Debug("H sync session", "id", id.String())
			d := cho.db.NewBatch()
			cho.syncs.Store(id, d)
			err = cho.ApplyH(id, ref, body, d)

		case 'D': // diff
			cho.log.Debug("'D' sync session", "id", id.String())
			d, ok := cho.syncs.Load(id)
			if !ok {
				return ErrSyncUnknown
			}
			err = cho.ApplyD(id, ref, body, d)
			noApply = true

		case 'V':
			cho.log.Debug("V sync session", "id", id.String())
			d, ok := cho.syncs.Load(id)
			if !ok {
				return ErrSyncUnknown
			}
			err = cho.ApplyV(id, ref, body, d)
			if err == nil {
				err = cho.db.Apply(d, &pebbleWriteOptions)
				cho.syncs.Delete(id)
			}
			noApply = true

		case 'B': // bye dear
			cho.syncs.Delete(id)

		default:
			return fmt.Errorf("unsupported packet type %c", lit)
		}

		if !noApply && err == nil {
			if err := cho.db.Apply(&pb, &pebbleWriteOptions); err != nil {
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
