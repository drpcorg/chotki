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

// TLV all the way down
type Chotki struct {
	last rdx.ID
	src  uint64

	db  *pebble.DB
	dir string

	syncs map[rdx.ID]*pebble.Batch

	// queues to broadcast all new packets
	outq toyqueue.RecordQueue
	fan  toyqueue.Fanout

	lock   sync.Mutex
	idlock sync.Mutex

	tcp toytlv.TCPDepot

	opts Options

	types map[rdx.ID]string
}

var ErrCausalityBroken = errors.New("order fail: refs an unknown op")
var ErrOutOfOrder = errors.New("order fail: sequence gap")

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
func (ch *Chotki) Source() uint64 {
	return ch.src
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
		vals: append([][]byte{value}),
	}
	return &pma, nil
}

// Create a replica. orig=0 for read-only replicas.
func (ch *Chotki) Create(orig uint64, name string) (err error) {
	opts := pebble.Options{
		ErrorIfExists:    true,
		ErrorIfNotExists: false,
		Merger: &pebble.Merger{
			Name:  "CRDT",
			Merge: merger,
		}}
	ch.opts.SetDefaults() // todo param
	path := ReplicaDirName(orig)
	ch.db, err = pebble.Open(path, &opts)
	if err != nil {
		return
	}
	var _0 rdx.ID
	id0 := rdx.IDFromSrcSeqOff(orig, 0, 0)
	rec0 := toytlv.Concat(
		toytlv.Record('L',
			toytlv.Record('I', id0.ZipBytes()),
			toytlv.Record('R', _0.ZipBytes()),
			toytlv.Record('S', rdx.Stlv(name)),
		),
	)
	init := append(Log0, rec0)
	err = ch.Drain(init)
	if err != nil {
		return
	}
	_ = ch.Close()
	return ch.Open(orig)
}

// Open a replica. orig=0 for read-only replicas.
func (ch *Chotki) Open(orig uint64) (err error) {
	ch.src = orig
	opts := pebble.Options{
		ErrorIfNotExists: true,
		Merger: &pebble.Merger{
			Name:  "CRDT",
			Merge: merger,
		}}
	ch.opts.SetDefaults() // todo param
	path := ReplicaDirName(orig)
	ch.db, err = pebble.Open(path, &opts)
	if err != nil {
		return
	}
	ch.dir = path
	if err != nil {
		_ = ch.db.Close()
		return err
	}
	ch.types = make(map[rdx.ID]string)
	ch.syncs = make(map[rdx.ID]*pebble.Batch)
	var vv rdx.VV
	vv, err = ch.VersionVector()
	if err != nil {
		return
	}
	ch.last = vv.GetID(ch.src)
	// Host.last = Host.heads.GetID(orig) todo root VV
	//Host.inq.Limit = 8192
	/*Host.fan.Feeder = &Host.oqueue
	 */
	return
}

func (ch *Chotki) Feed() (toyqueue.Records, error) {
	// fixme multi if Host.fan.
	return ch.outq.Feed()
}

func (ch *Chotki) OpenTCP(tcp *toytlv.TCPDepot) {
	tcp.Open(func(conn net.Conn) toyqueue.FeedDrainCloser {
		// fixme snapshot etc
		return &Syncer{Host: ch}
	})
	// ...
	io := pebble.IterOptions{}
	i := ch.db.NewIter(&io)
	for i.SeekGE([]byte{'L'}); i.Valid() && i.Key()[0] == 'L'; i.Next() {
		address := string(i.Key()[1:])
		err := tcp.Listen(address)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
		}
	}
	for i.SeekGE([]byte{'C'}); i.Valid() && i.Key()[0] == 'C'; i.Next() {
		address := string(i.Key()[1:])
		err := tcp.Connect(address)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
		}
	}
	_ = i.Close()
}

func (ch *Chotki) AddPacketHose(hose toyqueue.DrainCloser) error {
	// todo open?
	ch.fan.AddDrain(hose)
	return nil
}

func (ch *Chotki) RemovePacketHose(hose toyqueue.DrainCloser) error {
	// todo return Host.fan.RemoveDrain(hose)
	return nil
}

func (ch *Chotki) warn(format string, a ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format, a...)
}

func (ch *Chotki) Drain(recs toyqueue.Records) (err error) {
	rest := recs
	apply := toyqueue.Records{}

	for len(rest) > 0 && err == nil { // parse the packets
		packet := rest[0]
		rest = rest[1:]
		lit, id, ref, body, err := ParsePacket(packet)
		if err != nil {
			ch.warn("bad packet: %s", err.Error())
			continue
		}
		apply = append(apply, packet)
		if id.Src() == ch.src && id > ch.last {
			if id.Off() != 0 {
				return rdx.ErrBadPacket
			}
			ch.last = id
		}
		yv := false
		pb := pebble.Batch{}
		switch lit {
		case 'L': // create replica log
			if ref != rdx.ID0 {
				return ErrBadLPacket
			}
			err = ch.ApplyLOT(id, ref, body, &pb)
		case 'O': // create object
			if ref == rdx.ID0 {
				return ErrBadLPacket
			}
			err = ch.ApplyLOT(id, ref, body, &pb)
		case 'T':
			err = ch.ApplyLOT(id, ref, body, &pb)
		case 'E':
			if ref == rdx.ID0 {
				return ErrBadLPacket
			}
			err = ch.ApplyE(id, ref, body, &pb)
		case 'Y':
			d, ok := ch.syncs[ref]
			if !ok {
				d = ch.db.NewBatch()
				ch.syncs[id] = d
			}
			err = ch.ApplyY(id, ref, body, d)
			yv = true
		case 'V':
			d, ok := ch.syncs[ref]
			if !ok {
				return ErrSyncUnknown
			}
			err = ch.ApplyV(id, ref, body, d)
			if err == nil {
				err = ch.db.Apply(d, &WriteOptions)
				delete(ch.syncs, ref)
			}
			yv = true
		default:
			_, _ = fmt.Fprintf(os.Stderr, "unsupported packet %c skipped\n", lit)
		}
		if !yv && err == nil {
			err = ch.db.Apply(&pb, &WriteOptions)
		}
	}
	if err != nil { // fixme separate packets
		return
	}

	// todo err = Host.oqueue.Drain(recs) // nonblocking

	return
}

func (ch *Chotki) VersionVector() (vv rdx.VV, err error) {
	key0 := VKey(rdx.ID0)
	val, clo, err := ch.db.Get(key0)
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

func (ch *Chotki) Last() rdx.ID {
	return ch.last
}

func (ch *Chotki) Close() error {
	if ch.db == nil {
		return ErrClosed
	}
	_ = ch.db.Close()
	ch.db = nil
	// todo
	return nil
}

func Join(records ...[]byte) (ret []byte) {
	for _, rec := range records {
		ret = append(ret, rec...)
	}
	return
}

// Here new packets are timestamped and queued for save
func (ch *Chotki) CommitPacket(lit byte, ref rdx.ID, body toyqueue.Records) (id rdx.ID, err error) {
	ch.idlock.Lock()
	id = (ch.last & ^rdx.OffMask) + rdx.ProInc
	i := toytlv.Record('I', id.ZipBytes())
	r := toytlv.Record('R', ref.ZipBytes())
	packet := toytlv.Record(lit, i, r, Join(body...))
	err = ch.Drain(toyqueue.Records{packet})
	ch.idlock.Unlock()
	return
}

func (ch *Chotki) ObjectKeyRange(oid rdx.ID) (fro, til []byte) {
	oid = oid & ^rdx.OffMask
	return OKey(oid, 0), OKey(oid+rdx.ProInc, 0xff)
}

// returns nil for "not found"
func (ch *Chotki) ObjectIterator(oid rdx.ID) *pebble.Iterator {
	fro, til := ch.ObjectKeyRange(oid)
	io := pebble.IterOptions{
		LowerBound: fro,
		UpperBound: til,
	}
	ret := ch.db.NewIter(&io)
	if ret.SeekGE(fro) {
		return ret
	} else {
		_ = ret.Close()
		return nil
	}
}
