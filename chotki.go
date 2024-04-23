package chotki

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
)

type Packet []byte

// Batch of packets
type Batch [][]byte

type Options struct {
	pebble.Options

	Orig         uint64
	Name         string
	RelaxedOrder bool
	MaxLogLen    int64
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

	db  *pebble.DB
	dir string

	syncs map[rdx.ID]*pebble.Batch
	hooks map[rdx.ID][]Hook
	hlock sync.Mutex

	// queues to broadcast all new packets
	outq map[string]toyqueue.DrainCloser

	outlock sync.Mutex
	lock    sync.Mutex

	opts Options

	orm ORM

	types map[rdx.ID]Fields
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

func ReplicaDirName(rno uint64) string {
	return fmt.Sprintf("cho%x", rno)
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

	conn := Chotki{
		db:    db,
		src:   opts.Orig,
		dir:   dirname,
		opts:  opts,
		types: make(map[rdx.ID]Fields),
		hooks: make(map[rdx.ID][]Hook),
		syncs: make(map[rdx.ID]*pebble.Batch),
		outq:  make(map[string]toyqueue.DrainCloser),
	}

	if !exists {
		id0 := rdx.IDFromSrcSeqOff(opts.Orig, 0, 0)

		init := append(toyqueue.Records(nil), Log0...)
		init = append(init, toytlv.Record('Y',
			toytlv.Record('I', id0.ZipBytes()),
			toytlv.Record('R', rdx.ID0.ZipBytes()),
			toytlv.Record('S', rdx.Stlv(opts.Name)),
		))

		if err = conn.Drain(init); err != nil {
			return nil, err
		}
	}

	vv, err := conn.VersionVector()
	if err != nil {
		return nil, err
	}

	conn.last = vv.GetID(conn.src)

	return &conn, nil
}

func (cho *Chotki) OpenTCP(tcp *toytlv.TCPDepot) error {
	if cho.db == nil {
		return ErrDbClosed
	}

	tcp.Open(func(conn net.Conn) toyqueue.FeedDrainCloser {
		return &Syncer{Host: cho, Name: conn.RemoteAddr().String(), Mode: SyncRWLive}
	})

	return nil
}

func (cho *Chotki) ReOpenTCP(tcp *toytlv.TCPDepot) error {
	if cho.db == nil {
		return ErrDbClosed
	}

	if err := cho.OpenTCP(tcp); err != nil {
		return err
	}
	// ...
	io := pebble.IterOptions{}
	i := cho.db.NewIter(&io)
	defer i.Close()

	for i.SeekGE([]byte{'l'}); i.Valid() && i.Key()[0] == 'L'; i.Next() {
		address := string(i.Key()[1:])
		err := tcp.Listen(address)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
		}
	}
	for i.SeekGE([]byte{'c'}); i.Valid() && i.Key()[0] == 'C'; i.Next() {
		address := string(i.Key()[1:])
		err := tcp.Connect(address)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
		}
	}
	return nil
}

func (cho *Chotki) AddPacketHose(name string) (feed toyqueue.FeedCloser) {
	queue := toyqueue.RecordQueue{Limit: SyncOutQueueLimit}
	cho.outlock.Lock()
	q := cho.outq[name]
	cho.outq[name] = &queue
	cho.outlock.Unlock()
	if q != nil {
		_, _ = fmt.Fprintf(os.Stderr, "closing the old conn to %s\n", name)
		_ = q.Close()
	}
	return queue.Blocking()
}

func (cho *Chotki) RemovePacketHose(name string) error {
	cho.outlock.Lock()
	q := cho.outq[name]
	delete(cho.outq, name)
	cho.outlock.Unlock()
	if q != nil {
		_, _ = fmt.Fprintf(os.Stderr, "closing the conn to %s\n", name)
		_ = q.Close()
	}
	return nil
}

func (cho *Chotki) Broadcast(records toyqueue.Records, except string) {
	cho.outlock.Lock()
	for name, hose := range cho.outq {
		if name == except {
			continue
		}
		err := hose.Drain(records)
		if err != nil {
			delete(cho.outq, name)
		}
	}
	cho.outlock.Unlock()
}

func (cho *Chotki) warn(format string, a ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format, a...)
}

func (cho *Chotki) Drain(recs toyqueue.Records) (err error) {
	rest := recs
	var calls []CallHook

	for len(rest) > 0 && err == nil { // parse the packets
		packet := rest[0]
		rest = rest[1:]
		lit, id, ref, body, parse_err := ParsePacket(packet)
		if parse_err != nil {
			cho.warn("bad packet: %s", parse_err.Error())
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
			err = cho.ApplyC(id, ref, body, &pb)
		case 'O': // create object
			if ref == rdx.ID0 {
				return ErrBadLPacket
			}
			err = cho.ApplyOY('O', id, ref, body, &pb)
		case 'Y': // create replica log
			if ref != rdx.ID0 {
				return ErrBadLPacket
			}
			err = cho.ApplyOY('Y', id, ref, body, &pb)
		case 'E':
			if ref == rdx.ID0 {
				return ErrBadLPacket
			}
			err = cho.ApplyE(id, ref, body, &pb, &calls)
		case 'H':
			fmt.Printf("H sync session %s\n", id.String())
			d := cho.db.NewBatch()
			cho.syncs[id] = d
			err = cho.ApplyH(id, ref, body, d)
		case 'D':
			fmt.Printf("D sync session %s\n", id.String())
			d, ok := cho.syncs[id]
			if !ok {
				return ErrSyncUnknown
			}
			err = cho.ApplyD(id, ref, body, d)
			yvh = true
		case 'V':
			fmt.Printf("V sync session %s\n", id.String())
			d, ok := cho.syncs[id]
			if !ok {
				return ErrSyncUnknown
			}
			err = cho.ApplyV(id, ref, body, d)
			if err == nil {
				err = cho.db.Apply(d, &WriteOptions)
				delete(cho.syncs, id)
			}
			yvh = true
		case 'B': // bye dear
			delete(cho.syncs, id)
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
	if cho.db == nil {
		cho.lock.Unlock()
		return ErrClosed
	}
	if err := cho.db.Close(); err != nil {
		return err
	}
	cho.db = nil
	// todo
	cho.last = rdx.ID0
	cho.lock.Unlock()
	return nil
}

func Join(records ...[]byte) (ret []byte) {
	for _, rec := range records {
		ret = append(ret, rec...)
	}
	return
}

// Here new packets are timestamped and queued for save
func (cho *Chotki) CommitPacket(lit byte, ref rdx.ID, body toyqueue.Records) (id rdx.ID, err error) {
	cho.lock.Lock()
	if cho.db == nil {
		cho.lock.Unlock()
		return rdx.BadId, ErrClosed
	}
	id = (cho.last & ^rdx.OffMask) + rdx.ProInc
	i := toytlv.Record('I', id.ZipBytes())
	r := toytlv.Record('R', ref.ZipBytes())
	packet := toytlv.Record(lit, i, r, Join(body...))
	recs := toyqueue.Records{packet}
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
	cho.hlock.Lock()
	list := cho.hooks[fid]
	list = append(list, hook)
	cho.hooks[fid] = list
	cho.hlock.Unlock()
}

func (cho *Chotki) RemoveAllHooks(fid rdx.ID) {
	cho.hlock.Lock()
	delete(cho.hooks, fid)
	cho.hlock.Unlock()
}

func (cho *Chotki) RemoveHook(fid rdx.ID, hook Hook) (err error) {
	cho.hlock.Lock()
	list := cho.hooks[fid]
	new_list := make([]Hook, 0, len(list))
	for _, h := range list {
		if &h != &hook {
			new_list = append(new_list, h)
		}
	}
	if len(new_list) == len(list) {
		err = ErrHookNotFound
	}
	cho.hooks[fid] = list
	cho.hlock.Unlock()
	return
}

func (cho *Chotki) ObjectMapper() *ORM {
	if cho.orm.Snap == nil {
		cho.orm.Host = cho
		cho.orm.Snap = cho.db.NewSnapshot()
		cho.orm.objects = make(map[rdx.ID]NativeObject)
		cho.orm.ids = make(map[NativeObject]rdx.ID)
	}
	return &cho.orm
}
