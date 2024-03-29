package chotki

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
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
	last ID
	src  uint64

	db  *pebble.DB
	dir string

	syncs map[ID]*pebble.Batch

	// queues to broadcast all new packets
	outq toyqueue.RecordQueue
	fan  toyqueue.Feeder2Drainers

	lock   sync.Mutex
	idlock sync.Mutex

	tcp toytlv.TCPDepot

	opts Options

	types map[ID]string
}

var ErrCausalityBroken = errors.New("order fail: refs an unknown op")
var ErrOutOfOrder = errors.New("order fail: sequence gap")

func OKey(id ID, rdt byte) (key []byte) {
	var ret = [16]byte{'O'}
	key = binary.BigEndian.AppendUint64(ret[:1], uint64(id))
	key = append(key, rdt)
	return
}

const LidLKeyLen = 1 + 8 + 1

func OKeyIdRdt(key []byte) (id ID, rdt byte) {
	if len(key) != LidLKeyLen {
		return BadId, 0
	}
	rdt = key[LidLKeyLen-1]
	id = IDFromBytes(key[1 : LidLKeyLen-1])
	return
}

func VKey(id ID) (key []byte) {
	var ret = [16]byte{'V'}
	block := id & ^VBlockMask
	key = binary.BigEndian.AppendUint64(ret[:1], uint64(block))
	key = append(key, 'V')
	return
}

func VKeyId(key []byte) ID {
	if len(key) != LidLKeyLen {
		return BadId
	}
	return IDFromBytes(key[1:])
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
	var _0 ID
	id0 := IDFromSrcSeqOff(orig, 0, 0)
	rec0 := toytlv.Concat(
		toytlv.Record('L',
			toytlv.Record('I', id0.ZipBytes()),
			toytlv.Record('R', _0.ZipBytes()),
			toytlv.Record('S', Stlv(name)),
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
	ch.types = make(map[ID]string)
	ch.syncs = make(map[ID]*pebble.Batch)
	var vv VV
	vv, err = ch.VersionVector()
	if err != nil {
		return
	}
	ch.last = vv.GetID(ch.src)
	// ch.last = ch.heads.GetID(orig) todo root VV
	//ch.inq.Limit = 8192
	/*ch.fan.Feeder = &ch.outq
	 */
	return
}

func (ch *Chotki) Feed() (toyqueue.Records, error) {
	// fixme multi if ch.fan.
	return ch.outq.Feed()
}

func (ch *Chotki) OpenTCP(tcp *toytlv.TCPDepot) {
	tcp.Open(func(conn net.Conn) toyqueue.FeedDrainCloser {
		return &Baker{ch: ch}
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
	// todo return ch.fan.RemoveDrain(hose)
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
				return ErrBadPacket
			}
			ch.last = id
		}
		yv := false
		pb := pebble.Batch{}
		switch lit {
		case 'L': // create replica log
			if ref != ID0 {
				return ErrBadLPacket
			}
			err = ch.ApplyLOT(id, ref, body, &pb)
		case 'O': // create object
			if ref == ID0 {
				return ErrBadLPacket
			}
			err = ch.ApplyLOT(id, ref, body, &pb)
		case 'T':
			err = ch.ApplyLOT(id, ref, body, &pb)
		case 'E':
			if ref == ID0 {
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

	// todo err = ch.outq.Drain(recs) // nonblocking

	return
}

func (ch *Chotki) VersionVector() (vv VV, err error) {
	key0 := VKey(ID0)
	val, clo, err := ch.db.Get(key0)
	if err == nil {
		vv = make(VV)
		err = vv.PutTLV(val)
	}
	if clo != nil {
		_ = clo.Close()
	}
	return
}

var WriteOptions = pebble.WriteOptions{Sync: false}

var ErrBadIRecord = errors.New("bad id-ref record")

var ErrBadPacket = errors.New("bad packet")
var ErrBadEPacket = errors.New("bad E packet")
var ErrBadVPacket = errors.New("bad V packet")
var ErrBadYPacket = errors.New("bad Y packet")
var ErrBadLPacket = errors.New("bad L packet")
var ErrBadTPacket = errors.New("bad T packet")
var ErrBadOPacket = errors.New("bad O packet")
var ErrSrcUnknown = errors.New("source unknown")
var ErrSyncUnknown = errors.New("sync session unknown")
var ErrBadRRecord = errors.New("bad ref record")

var KeyLogLen = []byte("Mloglen")

func (ch *Chotki) Close() error {
	_ = ch.db.Close()
	return nil
}

func Join(records ...[]byte) (ret []byte) {
	for _, rec := range records {
		ret = append(ret, rec...)
	}
	return
}

// Here new packets are timestamped and queued for save
func (ch *Chotki) CommitPacket(lit byte, ref ID, body toyqueue.Records) (id ID, err error) {
	ch.idlock.Lock()
	id = (ch.last & ^OffMask) + ProInc
	i := toytlv.Record('I', id.ZipBytes())
	r := toytlv.Record('R', ref.ZipBytes())
	packet := toytlv.Record(lit, i, r, Join(body...))
	err = ch.Drain(toyqueue.Records{packet})
	ch.idlock.Unlock()
	return
}

func (ch *Chotki) ObjectKeyRange(oid ID) (fro, til []byte) {
	oid = oid & ^OffMask
	return OKey(oid, 0), OKey(oid+ProInc, 0xff)
}

// returns nil for "not found"
func (ch *Chotki) ObjectIterator(oid ID) *pebble.Iterator {
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
