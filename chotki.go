package chotki

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/toytlv"
	"github.com/drpcorg/chotki/utils"
	"github.com/puzpuzpuz/xsync/v3"
)

type Packet []byte
type Batch []Packet

type Options struct {
	pebble.Options

	Src          uint64
	Name         string
	Log1         utils.Records
	MaxLogLen    int64
	RelaxedOrder bool

	TlsCertFile string
	TlsKeyFile  string
}

func (o *Options) SetDefaults() {
	if o.MaxLogLen == 0 {
		o.MaxLogLen = 1 << 23
	}

	if o.Merger == nil {
		o.Merger = &pebble.Merger{Name: "CRDT", Merge: merger}
	}
}

type Hook func(cho *Chotki, id rdx.ID) error

type CallHook struct {
	hook Hook
	id   rdx.ID
}

// TLV all the way down
type Chotki struct {
	last rdx.ID
	src  uint64

	lock sync.Mutex
	db   *pebble.DB
	orm  *ORM
	net  *toytlv.Transport
	dir  string
	opts Options
	log  utils.Logger

	outq  *xsync.MapOf[string, utils.DrainCloser] // queues to broadcast all new packets
	syncs *xsync.MapOf[rdx.ID, *pebble.Batch]
	hooks *xsync.MapOf[rdx.ID, []Hook]
	types *xsync.MapOf[rdx.ID, Fields]
}

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

	ErrOutOfOrder      = errors.New("chotki: order fail: sequence gap")
	ErrCausalityBroken = errors.New("chotki: order fail: refs an unknown op")
)

func OKey(id rdx.ID, rdt byte) (key []byte) {
	var ret = [16]byte{'O'}
	key = binary.BigEndian.AppendUint64(ret[:1], uint64(id))
	key = append(key, rdt)
	return
}

const LidLKeyLen = 1 + 8 + 1

func OKeyIdRdt(key []byte) (id rdx.ID, rdt byte) {
	if len(key) != LidLKeyLen {
		return rdx.BadId, 0
	}
	rdt = key[LidLKeyLen-1]
	id = rdx.IDFromBytes(key[1 : LidLKeyLen-1])
	return
}

func VKey(id rdx.ID) (key []byte) {
	var ret = [16]byte{'V'}
	block := id & ^SyncBlockMask
	key = binary.BigEndian.AppendUint64(ret[:1], uint64(block))
	key = append(key, 'V')
	return
}

func VKeyId(key []byte) rdx.ID {
	if len(key) != LidLKeyLen {
		return rdx.BadId
	}
	return rdx.IDFromBytes(key[1:])
}

// ToyKV convention key, lit O, then O00000-00000000-000 id
func (cho *Chotki) Source() uint64 {
	return cho.src
}

func merger(key, value []byte) (pebble.ValueMerger, error) {
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
}

func (cho *Chotki) Database() *pebble.DB {
	return cho.db
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
		opts: opts,

		log: utils.NewDefaultLogger(slog.LevelWarn),

		outq:  xsync.NewMapOf[string, utils.DrainCloser](),
		syncs: xsync.NewMapOf[rdx.ID, *pebble.Batch](),
		hooks: xsync.NewMapOf[rdx.ID, []Hook](),
		types: xsync.NewMapOf[rdx.ID, Fields](),
	}

	cho.net = toytlv.NewTransport(cho.log, func(conn net.Conn) utils.FeedDrainCloser {
		return &Syncer{
			Host: &cho,
			Mode: SyncRWLive,
			Name: conn.RemoteAddr().String(),
			Log:  cho.log,
		}
	})
	cho.net.CertFile = opts.TlsCertFile
	cho.net.KeyFile = opts.TlsKeyFile

	cho.orm = NewORM(&cho, db.NewSnapshot())

	if !exists {
		id0 := rdx.IDFromSrcSeqOff(opts.Src, 0, 0)

		init := append(utils.Records(nil), Log0...)
		init = append(init, opts.Log1...)
		init = append(init, toytlv.Record('Y',
			toytlv.Record('I', id0.ZipBytes()),
			toytlv.Record('R', rdx.ID0.ZipBytes()),
			toytlv.Record('S', rdx.Stlv(opts.Name)),
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

func (cho *Chotki) AddPacketHose(name string) (feed utils.FeedCloser) {
	if q, deleted := cho.outq.LoadAndDelete(name); deleted && q != nil {
		cho.log.Warn(fmt.Sprintf("closing the old conn to %s", name))
		if err := q.Close(); err != nil {
			cho.log.Error(fmt.Sprintf("couldn't close conn %s", name), "err", err)
		}
	}

	queue := utils.NewRecordQueue(SyncOutQueueLimit, time.Millisecond)
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

func (cho *Chotki) Broadcast(records utils.Records, except string) {
	cho.outq.Range(func(name string, hose utils.DrainCloser) bool {
		if name != except {
			if err := hose.Drain(records); err != nil {
				cho.outq.Delete(name)
			}
		}

		return true
	})
}

func (cho *Chotki) Drain(recs utils.Records) (err error) {
	rest := recs
	var calls []CallHook

	for len(rest) > 0 && err == nil { // parse the packets
		packet := rest[0]
		rest = rest[1:]
		lit, id, ref, body, parse_err := ParsePacket(packet)
		if parse_err != nil {
			cho.log.Warn("bad packet", "err", parse_err)
			return parse_err
		}
		if id.Src() == cho.src && id > cho.last {
			if id.Off() != 0 {
				return rdx.ErrBadPacket
			}
			cho.last = id
		}
		yvh := false
		pb := pebble.Batch{}
		switch lit {
		case 'C':
			cho.log.Debug("C sync session", "id", id.String())
			err = cho.ApplyC(id, ref, body, &pb)

		case 'O': // create object
			cho.log.Debug("O sync session", "id", id.String())
			if ref == rdx.ID0 {
				return ErrBadLPacket
			}
			err = cho.ApplyOY('O', id, ref, body, &pb)

		case 'Y': // create replica log
			cho.log.Debug("Y sync session", "id", id.String())
			if ref != rdx.ID0 {
				return ErrBadLPacket
			}
			err = cho.ApplyOY('Y', id, ref, body, &pb)

		case 'E':
			cho.log.Debug("E sync session", "id", id.String())
			if ref == rdx.ID0 {
				return ErrBadLPacket
			}
			err = cho.ApplyE(id, ref, body, &pb, &calls)

		case 'H':
			cho.log.Debug("H sync session", "id", id.String())
			d := cho.db.NewBatch()
			cho.syncs.Store(id, d)
			err = cho.ApplyH(id, ref, body, d)

		case 'D':
			cho.log.Debug("D sync session", "id", id.String())
			d, ok := cho.syncs.Load(id)
			if !ok {
				return ErrSyncUnknown
			}
			err = cho.ApplyD(id, ref, body, d)
			yvh = true

		case 'V':
			cho.log.Debug("V sync session", "id", id.String())
			d, ok := cho.syncs.Load(id)
			if !ok {
				return ErrSyncUnknown
			}
			err = cho.ApplyV(id, ref, body, d)
			if err == nil {
				err = cho.db.Apply(d, &WriteOptions)
				cho.syncs.Delete(id)
			}
			yvh = true

		case 'B': // bye dear
			cho.syncs.Delete(id)

		default:
			return fmt.Errorf("unsupported packet type %c", lit)
		}

		if !yvh && err == nil {
			if err := cho.db.Apply(&pb, &WriteOptions); err != nil {
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

var WriteOptions = pebble.WriteOptions{Sync: true}

var KeyLogLen = []byte("Mloglen")

func (cho *Chotki) Last() rdx.ID {
	return cho.last
}

func (cho *Chotki) Snapshot() pebble.Reader {
	return cho.db.NewSnapshot()
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

func Join(records ...[]byte) (ret []byte) {
	for _, rec := range records {
		ret = append(ret, rec...)
	}
	return
}

// Here new packets are timestamped and queued for save
func (cho *Chotki) CommitPacket(lit byte, ref rdx.ID, body utils.Records) (id rdx.ID, err error) {
	cho.lock.Lock()
	if cho.db == nil {
		cho.lock.Unlock()
		return rdx.BadId, ErrClosed
	}
	id = (cho.last & ^rdx.OffMask) + rdx.ProInc
	i := toytlv.Record('I', id.ZipBytes())
	r := toytlv.Record('R', ref.ZipBytes())
	packet := toytlv.Record(lit, i, r, Join(body...))
	recs := utils.Records{packet}
	err = cho.Drain(recs)
	cho.Broadcast(recs, "")
	cho.lock.Unlock()
	return
}

func ObjectKeyRange(oid rdx.ID) (fro, til []byte) {
	oid = oid & ^rdx.OffMask
	return OKey(oid, 'O'), OKey(oid+rdx.ProInc, 0)
}

func ObjectIterator(oid rdx.ID, snap *pebble.Snapshot) (it *pebble.Iterator, err error) {
	fro, til := ObjectKeyRange(oid)
	io := pebble.IterOptions{
		LowerBound: fro,
		UpperBound: til,
	}
	it = snap.NewIter(&io)
	if it.SeekGE(fro) { // fixme
		id, rdt := OKeyIdRdt(it.Key())
		if rdt == 'O' && id == oid {
			// An iterator is returned from a function, it cannot be closed
			return it, nil
		}
	}
	if it != nil {
		_ = it.Close()
	}
	return nil, ErrObjectUnknown
}

// returns nil for "not found"
func (cho *Chotki) ObjectIterator(oid rdx.ID) *pebble.Iterator {
	fro, til := ObjectKeyRange(oid)
	io := pebble.IterOptions{
		LowerBound: fro,
		UpperBound: til,
	}
	it := cho.db.NewIter(&io)
	if it.SeekGE(fro) {
		id, rdt := OKeyIdRdt(it.Key())
		if rdt == 'O' && id == oid {
			// An iterator is returned from a function, it cannot be closed
			return it
		}
	}
	if it != nil {
		_ = it.Close()
	}
	return nil
}

func (cho *Chotki) GetFieldTLV(id rdx.ID) (rdt byte, tlv []byte) {
	return GetFieldTLV(cho.db, id)
}

func GetFieldTLV(reader pebble.Reader, id rdx.ID) (rdt byte, tlv []byte) {
	key := OKey(id, 'A')
	it := reader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{'O'},
		UpperBound: []byte{'P'},
	})
	defer it.Close()
	if it.SeekGE(key) {
		fact, r := OKeyIdRdt(it.Key())
		if fact == id {
			tlv = it.Value()
			rdt = r
		}
	}
	return
}

func (cho *Chotki) AddHook(fid rdx.ID, hook Hook) {
	cho.lock.Lock()
	defer cho.lock.Unlock()

	list, _ := cho.hooks.Load(fid)
	list = append(list, hook)
	cho.hooks.Store(fid, list)
}

func (cho *Chotki) RemoveAllHooks(fid rdx.ID) {
	cho.hooks.Delete(fid)
}

func (cho *Chotki) RemoveHook(fid rdx.ID, hook Hook) (err error) {
	cho.lock.Lock()
	defer cho.lock.Unlock()

	list, ok := cho.hooks.Load(fid)
	if !ok {
		return ErrHookNotFound
	}

	new_list := make([]Hook, 0, len(list))
	for _, h := range list {
		if &h != &hook {
			new_list = append(new_list, h)
		}
	}
	if len(new_list) == len(list) {
		err = ErrHookNotFound
	}
	cho.hooks.Store(fid, list)
	return
}

func (cho *Chotki) ObjectMapper() *ORM {
	return cho.orm
}
