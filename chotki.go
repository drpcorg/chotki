package chotki

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki/rdx"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"net"
	"os"
	"sync"
)

type Packet []byte

// Batch of packets
type Batch [][]byte

type Options struct {
	RelaxedOrder bool
	MaxLogLen    int64
}

type Hook func(id rdx.ID) error

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
	idlock  sync.Mutex

	opts Options

	types map[rdx.ID]Fields
}

var ErrCausalityBroken = errors.New("order fail: refs an unknown op")
var ErrOutOfOrder = errors.New("order fail: sequence gap")
var ErrNotImplemented = errors.New("not implemented yet")

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

var ErrAlreadyOpen = errors.New("the db is already open")

func (o *Options) SetDefaults() {
	if o.MaxLogLen == 0 {
		o.MaxLogLen = 1 << 23
	}
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

// Create a replica. orig=0 for read-only replicas.
func (cho *Chotki) Create(orig uint64, name string) (err error) {
	opts := pebble.Options{
		ErrorIfExists:    true,
		ErrorIfNotExists: false,
		Merger: &pebble.Merger{
			Name:  "CRDT",
			Merge: merger,
		}}
	cho.opts.SetDefaults() // todo param
	path := ReplicaDirName(orig)
	cho.db, err = pebble.Open(path, &opts)
	if err != nil {
		return
	}
	var _0 rdx.ID
	id0 := rdx.IDFromSrcSeqOff(orig, 0, 0)
	rec0 := toytlv.Concat(
		toytlv.Record('Y',
			toytlv.Record('I', id0.ZipBytes()),
			toytlv.Record('R', _0.ZipBytes()),
			toytlv.Record('S', rdx.Stlv(name)),
		),
	)
	init := append(Log0, rec0)
	err = cho.Drain(init)
	if err != nil {
		return
	}
	_ = cho.Close()
	return cho.Open(orig)
}

// Open a replica. orig=0 for read-only replicas.
func (cho *Chotki) Open(orig uint64) (err error) {
	cho.src = orig
	opts := pebble.Options{
		ErrorIfNotExists: true,
		Merger: &pebble.Merger{
			Name:  "CRDT",
			Merge: merger,
		}}
	cho.opts.SetDefaults() // todo param
	path := ReplicaDirName(orig)
	cho.db, err = pebble.Open(path, &opts)
	if err != nil {
		return
	}
	cho.dir = path
	if err != nil {
		_ = cho.db.Close()
		return err
	}
	cho.types = make(map[rdx.ID]Fields)
	cho.syncs = make(map[rdx.ID]*pebble.Batch)
	cho.hooks = make(map[rdx.ID][]Hook)
	var vv rdx.VV
	vv, err = cho.VersionVector()
	if err != nil {
		return
	}
	cho.last = vv.GetID(cho.src)
	cho.outq = make(map[string]toyqueue.DrainCloser)
	// repl.last = repl.heads.GetID(orig) todo root VV
	return
}

func (cho *Chotki) OpenTCP(tcp *toytlv.TCPDepot) {
	tcp.Open(func(conn net.Conn) toyqueue.FeedDrainCloser {
		return &Syncer{Host: cho, Name: conn.RemoteAddr().String(), Mode: SyncRWLive}
	})
}

func (cho *Chotki) ReOpenTCP(tcp *toytlv.TCPDepot) {
	cho.OpenTCP(tcp)
	// ...
	io := pebble.IterOptions{}
	i := cho.db.NewIter(&io)
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
	_ = i.Close()
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
			_ = cho.db.Apply(&pb, &WriteOptions)
		}
	}
	if err != nil { // fixme separate packets
		return
	}

	if len(calls) > 0 {
		go cho.fireCalls(calls)
	}

	return
}

func (cho *Chotki) fireCalls(calls []CallHook) {
	cho.hlock.Lock()
	for i := 0; i < len(calls); i++ {
		err := calls[i].hook(calls[i].id)
		if err != nil {
			calls[i] = calls[len(calls)-1]
			calls = calls[:len(calls)-1]
		}
	}
	cho.hlock.Unlock()
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

var WriteOptions = pebble.WriteOptions{Sync: false}

var ErrBadIRecord = errors.New("bad id-ref record")

var ErrBadHPacket = errors.New("bad handshake packet")
var ErrBadEPacket = errors.New("bad E packet")
var ErrBadVPacket = errors.New("bad V packet")
var ErrBadYPacket = errors.New("bad Y packet")
var ErrBadLPacket = errors.New("bad L packet")
var ErrBadTPacket = errors.New("bad T packet")
var ErrBadOPacket = errors.New("bad O packet")
var ErrSrcUnknown = errors.New("source unknown")
var ErrSyncUnknown = errors.New("sync session unknown")
var ErrBadRRecord = errors.New("bad ref record")
var ErrClosed = errors.New("no replica open")

var KeyLogLen = []byte("Mloglen")

func (cho *Chotki) Last() rdx.ID {
	return cho.last
}

func (cho *Chotki) Snapshot() pebble.Reader {
	return cho.db.NewSnapshot()
}

func (cho *Chotki) Close() error {
	if cho.db == nil {
		return ErrClosed
	}
	_ = cho.db.Close()
	cho.db = nil
	// todo
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
func (cho *Chotki) CommitPacket(lit byte, ref rdx.ID, body toyqueue.Records) (id rdx.ID, err error) {
	cho.idlock.Lock()
	id = (cho.last & ^rdx.OffMask) + rdx.ProInc
	i := toytlv.Record('I', id.ZipBytes())
	r := toytlv.Record('R', ref.ZipBytes())
	packet := toytlv.Record(lit, i, r, Join(body...))
	recs := toyqueue.Records{packet}
	err = cho.Drain(recs)
	cho.Broadcast(recs, "")
	cho.idlock.Unlock()
	return
}

func (cho *Chotki) ObjectKeyRange(oid rdx.ID) (fro, til []byte) {
	oid = oid & ^rdx.OffMask
	return OKey(oid, 'O'), OKey(oid+rdx.ProInc, 0)
}

// returns nil for "not found"
func (cho *Chotki) ObjectIterator(oid rdx.ID) *pebble.Iterator {
	fro, til := cho.ObjectKeyRange(oid)
	io := pebble.IterOptions{
		LowerBound: fro,
		UpperBound: til,
	}
	it := cho.db.NewIter(&io)
	if it.SeekGE(fro) {
		id, rdt := OKeyIdRdt(it.Key())
		if rdt == 'O' && id == oid {
			return it
		}
	}
	_ = it.Close()
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
	if it.SeekGE(key) {
		fact, r := OKeyIdRdt(it.Key())
		if fact == id {
			tlv = it.Value()
			rdt = r
		}
	}
	_ = it.Close()
	return
}

func (cho *Chotki) AddHook(fid rdx.ID, hook Hook) {
	cho.hlock.Lock()
	list := cho.hooks[fid]
	list = append(list, hook)
	cho.hooks[fid] = list
	cho.hlock.Unlock()
}

var ErrHookNotFound = errors.New("hook not found")

func (cho *Chotki) RemoveHook(fid rdx.ID, hook Hook) (err error) {
	cho.hlock.Lock()
	list := cho.hooks[fid]
	i := 0
	for i < len(list) {
		if &list[i] == &hook { // todo ?
			list[i] = list[len(list)-1]
			list = list[:len(list)-1]
			break
		}
		i++
	}
	if i == len(list) {
		err = ErrHookNotFound
	}
	cho.hooks[fid] = list
	cho.hlock.Unlock()
	return
}
