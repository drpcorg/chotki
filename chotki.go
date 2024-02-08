package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/learn-decentralized-systems/toykv"
	"github.com/learn-decentralized-systems/toylog"
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
}

// TLV all the way down
type Chotki struct {
	db    toykv.KeyValueStore
	log   toylog.ChunkedLog
	tcp   toytlv.TCPDepot
	heads VV
	// inq is the incoming packet queue; bakers dump incoming packets here
	inq toyqueue.RecordQueue
	// outq contains outgoing queues; here we broadcast new packets
	outq   []toyqueue.DrainCloser
	evoutq toyqueue.RecordQueue
	evqs   map[ID]toyqueue.DrainCloser
	last   ID
	src    uint32
	lock   sync.Mutex
	opts   Options
}

var ErrCausalityBroken = errors.New("order fail: refs an unknown op")
var ErrOutOfOrder = errors.New("order fail: sequence gap")

// ToyKV convention key, lit O, then O00000-00000000-000 id
func OKey(id ID) []byte {
	var ret = [32]byte{'O'}
	return id.Hex583(ret[:1])
}

func ReplicaFilename(rno uint32) string {
	return fmt.Sprintf("cho%d", rno)
}

func (ch *Chotki) Open(orig uint32) (err error) {
	opts := pebble.Options{
		DisableWAL:       true,
		ErrorIfNotExists: false,
		Merger: &pebble.Merger{
			Name: "CRDT",
			Merge: func(key, value []byte) (pebble.ValueMerger, error) {
				pma := PebbleMergeAdaptor{
					key: key,
				}
				_ = pma.MergeOlder(value)
				return &pma, nil
			},
		}}
	path := ReplicaFilename(orig)
	ch.db.DB, err = pebble.Open(path, &opts)
	if err != nil {
		return
	}
	ch.heads = make(map[uint32]uint32)
	ch.log.Header = &VVFeeder{ // todo limits
		vv:   ch.heads,
		lock: &ch.lock,
	}
	err = ch.log.Open(path)
	if err == nil {
		err = ch.RecoverProgress()
	}
	if err != nil {
		_ = ch.db.DB.Close()
		return err
	}
	ch.last = ch.heads.GetLastID(orig)
	ch.src = orig
	ch.inq.Limit = 8192
	ch.tcp.Open(func(conn net.Conn) toyqueue.FeedDrainCloser {
		return &MasterBaker{replica: ch}
	})
	ch.RecoverConnects()
	go ch.DoProcessPacketQueue()
	return
}

func (re *Chotki) RecoverProgress() error {
	reader, err := re.log.Reader(0, toylog.ChunkSeekEnd)
	if err != nil {
		return err
	}
	feeder := toytlv.FeedSeekCloser{Reader: reader}
	recs, err := feeder.Feed()
	if err == nil {
		err = re.heads.LoadBytes(recs[0])
	}
	if err != nil {
		return err
	}
	for {
		recs, err = feeder.Feed()
		if len(recs) == 0 || err != nil {
			break
		}
		for _, rec := range recs {
			src, seq := PacketSrcSeq(rec)
			re.heads.Put(seq, src)
		}
	}
	return nil
}

func (re *Chotki) RecoverConnects() {
	// ...
	io := pebble.IterOptions{}
	i := re.db.DB.NewIter(&io)
	for i.SeekGE([]byte{'L'}); i.Valid() && i.Key()[0] == 'L'; i.Next() {
		address := string(i.Key()[1:])
		err := re.tcp.Listen(address)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
		}
	}
	for i.SeekGE([]byte{'C'}); i.Valid() && i.Key()[0] == 'C'; i.Next() {
		address := string(i.Key()[1:])
		err := re.tcp.Connect(address)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err.Error())
		}
	}
	_ = i.Close()
}

func (ch *Chotki) AddPacketHose(hose toyqueue.DrainCloser) {
	ch.lock.Lock()
	ch.outq = append(ch.outq, hose)
	ch.lock.Unlock()
}

func (ch *Chotki) RemovePacketHose(hose toyqueue.DrainCloser) {
	ch.lock.Lock()
	newlist := make([]toyqueue.DrainCloser, 0, len(ch.outq))
	for _, o := range ch.outq {
		if o != hose {
			newlist = append(newlist, o)
		}
	}
	ch.outq = newlist
	ch.lock.Unlock()
}

// Ignores already-seen; returns ErrGap on sequence gaps
func FilterPackets(vv VV, batch toyqueue.Records) (new_batch toyqueue.Records, err error) {
	var _ignored [32]int
	ignored := _ignored[0:0:32]
	for i, packet := range batch {
		src, seq := PacketSrcSeq(packet)
		head, _ := vv[src]
		if head >= seq {
			ignored = append(ignored, i)
		} else if head+1 < seq {
			err = ErrGap // may be unacceptable
			ignored = append(ignored, i)
		}
	}
	if len(ignored) == 0 {
		return batch, nil
	}
	new_batch = make(toyqueue.Records, 0, len(batch)-len(ignored))
	for i, packet := range batch {
		if len(ignored) == 0 || i != ignored[0] {
			new_batch = append(new_batch, packet)
		} else {
			ignored = ignored[1:]
		}
	}
	return new_batch, err
}

func (ch *Chotki) ProcessPackets(recs toyqueue.Records) (err error) {
	pb := pebble.Batch{}
	pack, _ := FilterPackets(ch.heads, recs) // here we ignore gaps :/
	rest := pack
	for len(rest) > 0 && err == nil {
		packet := rest[0]
		rest = rest[1:]
		lit := toytlv.Lit(packet)
		switch lit {
		case 'O':
			err = ch.ParseNewObject(packet, &pb)
		case 'E':
			err = ch.ParseEdits(packet, &pb)
		default:
			_, _ = fmt.Fprintf(os.Stderr, "unsupported packet %c skipped\n", lit)
		}
	}
	if err != nil {
		return
	}
	err = ch.log.Drain(pack)
	if err != nil {
		return
	}
	err = ch.log.Sync() // todo Commit() ?!!
	if err != nil {
		return
	}
	total := uint64(ch.log.TotalSize())
	err = pb.Set(KeyLogSize, ZipUint64(total), &NonSync)
	if err != nil {
		return
	}
	// todo ch.last
	err = ch.db.DB.Apply(&pb, &NonSync)
	if err != nil {
		return
	}
	ch.lock.Lock()
	tmpout := ch.outq
	ch.lock.Unlock()
	for i := 0; i < len(tmpout); i++ { // fanout
		outq := tmpout[i]
		if outq == nil {
			continue
		}
		e := outq.Drain(recs) // nonblock
		if e != nil {
			ch.RemovePacketHose(outq)
		}
	}
	return
}

func (ch *Chotki) DoProcessPacketQueue() {
	q := ch.inq.Blocking()
	var err error
	for err == nil {
		recs, err := q.Feed()
		if err != nil {
			break // closed
		}
		err = ch.ProcessPackets(recs)
	}
	_, _ = fmt.Fprintf(os.Stderr, "packet processing fail %s", err.Error())
}

// Event:  [C][len][refid][field:][value][eventid]
func (ch *Chotki) ParseOp(lit byte, body []byte) error {
	refbytes := body[:8]
	ref := ID(binary.BigEndian.Uint64(refbytes))
	refseen, ok := ch.heads[ref.Src()]
	if !ok || refseen < ref.Seq() {
		return ErrCausalityBroken
	}
	idbytes := body[8:16]
	id := ID(binary.BigEndian.Uint64(idbytes))
	// check vv (ignore)
	seenseq, ok := ch.heads[id.Src()]
	if ok && seenseq+1 != id.Seq() {
		if seenseq >= id.Seq() {
			return ErrSeen
		}
		return ErrOutOfOrder
	}
	err := ch.db.Merge(lit, string(idbytes), string(body[8:])) // TODO batch -> Batch
	if err != nil {
		return err
	}
	// update vv
	ch.heads[id.Src()] = id.Seq()
	return nil
}

var NonSync = pebble.WriteOptions{Sync: false}

var ErrBadIRecord = errors.New("bad id-ref record")

func ReadRX(op []byte) (ref ID, exec, rest []byte, err error) {
	ref, rest, err = TakeIDWary('R', op)
	if err != nil {
		return
	}
	var lit byte
	lit, exec, rest = toytlv.TakeAnyRecord(rest)
	if lit < 'A' {
		err = ErrBadIRecord
	}
	return
}

var ErrBadOPacket = errors.New("bad O packet")
var ErrSrcUnknown = errors.New("source unknown")
var ErrBadRRecord = errors.New("bad ref record")

// I RX RX RX -> R IX IX IX  TODO type letter in the key
func (ch *Chotki) ParseNewObject(pack []byte, batch *pebble.Batch) (err error) {
	var id, tmf ID
	lit, rest, _ := toytlv.TakeAny(pack)
	if lit != 'O' {
		return ErrBadOPacket
	}
	id, rest, err = TakeIDWary('I', rest)
	if err != nil {
		return ErrBadOPacket
	}
	pos, ok := ch.heads[id.Src()]
	if !ch.opts.RelaxedOrder { // todo this goes to Baker?
		if !ok && id.Seq() != 0 {
			return ErrSrcUnknown
		}
		if ok && pos+1 != id.Seq() {
			return ErrGap
		}
	}
	ch.heads[id.Src()] = id.Seq() //FIXME all the vv api
	tmf, rest, err = TakeIDWary('R', rest)
	if err != nil {
		return ErrBadRRecord // todo?
	}
	//key := append(litObject[:1], ZipID(id)...)
	key := OKey(id)
	value := toytlv.Record('R', ZipID(tmf))
	err = batch.Merge(key, value, &NonSync)
	if err != nil {
		return err
	}
	xid := id
	var ref ID
	var xb []byte
	for len(rest) > 0 {
		xid++
		ref, xb, rest, err = ReadRX(rest)
		if err != nil {
			return
		}
		if ref <= ID(OffMask) {
			ref = id | ref // fixme; also xid; also optional R
		}
		key := OKey(ref)
		value = toytlv.Append(nil, 'I', xid.ZipBytes())
		value = append(value, xb...)
		err = batch.Merge(key, value, &NonSync)
		if err != nil {
			return
		}
	}
	vvkey := []byte{'V'} // FIXME WHICH MERGE OPERATOR?!!
	vvval := toytlv.Record('V', xid.ZipBytes())
	err = batch.Merge(vvkey, vvval, &NonSync)
	return
}

var ErrOffsetOpId = errors.New("op id is offset")

func (ch *Chotki) ParseEdits(pack []byte, batch *pebble.Batch) (err error) {
	lit, rest, _ := toytlv.TakeAny(pack)
	if lit != 'E' {
		return ErrBadOPacket
	}
	id, rest, err := TakeIDWary('I', rest)
	if err != nil {
		return err
	}
	if id.Off() != 0 {
		return ErrOffsetOpId
	}
	xid := id
	var ref ID
	var xb []byte
	for len(rest) > 0 {
		ref, xb, rest, err = ReadRX(rest)
		if err != nil {
			return
		}
		xid++
		key := OKey(ref)
		value := toytlv.Append(nil, 'I', xid.ZipBytes())
		value = append(value, xb...)
		err = batch.Merge(key, value, &NonSync)
		if err != nil {
			return
		}
	}
	vvkey := []byte{'V'}
	vvval := toytlv.Record('V', xid.ZipBytes())
	err = batch.Merge(vvkey, vvval, &NonSync)
	return
}

var KeyLogSize = []byte{'L'}

// todo batching batches
func (ch *Chotki) AbsorbBatch(pack Batch) (err error) {
	return err
}

func (ch *Chotki) Close() error {
	ch.db.Close()
	ch.tcp.Close()
	_ = ch.inq.Close() // stops packet processing
	_ = ch.log.Close()
	return nil
}

func (ch *Chotki) NewID() ID {
	ch.last += SeqOne
	return ch.last
}

func (ch *Chotki) CreateObject(initial RDT) (ID, error) {
	id := ch.NewID()
	diff := initial.Diff(nil)
	// TODO push id
	key := id.ZipBytes()
	err := ch.db.Merge('O', string(key), string(diff))
	// FIXME log
	// FIXME commit
	// FIXME read back
	return id, err
}

// FindObject navigates object fields recursively to reach the object
// at the specified path.
func (ch *Chotki) FindObject(root ID, path string, empty RDT) error {
	return nil
}

func (ch *Chotki) GetObject(id ID, empty RDT) error {
	state, err := ch.db.Get('O', string(id.ZipBytes()))
	if err != nil {
		return err
	}
	empty.Apply([]byte(state))
	return nil
}

func (ch *Chotki) MergeObject(id ID, changed RDT) error {
	key := string(id.ZipBytes())
	state, err := ch.db.Get('D', key)
	if err != nil {
		return err
	}
	diff := changed.Diff([]byte(state))
	// todo ch.NewID()
	err = ch.db.Merge('D', key, string(diff))
	return err
}

func (ch *Chotki) ObjectKeyRange(oid ID) (fro, til []byte) {
	return OKey(oid), OKey(oid | ID(OffMask))
}

func (ch *Chotki) ObjectIterator(oid ID) *pebble.Iterator {
	fro, til := ch.ObjectKeyRange(oid)
	io := pebble.IterOptions{
		LowerBound: fro,
		UpperBound: til,
	}
	ret := ch.db.DB.NewIter(&io)
	if ret.SeekGE(OKey(oid)) {
		return ret
	} else {
		_ = ret.Close()
		return nil
	}
}
