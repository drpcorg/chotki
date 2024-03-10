package main

import (
	"errors"
	"fmt"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"io"
	"sync"
)

const (
	ConnFresh = iota
	ConnHsSent
	ConnHsRecvd
	ConnRunning
	ConnClosing
	ConnClosed
)

type MasterBaker struct {
	state   int
	replica *Chotki
	peervv  VV
	loglen  int64
	lock    sync.Mutex
	cond    sync.Cond
	inq     toyqueue.Drainer
	err     error
}

func (mb *MasterBaker) Advance(new_state int) {
	fmt.Printf("%d state %d->%d\n", mb.replica.src, mb.state, new_state)
	mb.lock.Lock()
	mb.state = new_state
	mb.cond.Broadcast()
	mb.lock.Unlock()
}

func (mb *MasterBaker) Forward(new_state int) (recs toyqueue.Records, err error) {
	mb.Advance(new_state)
	return mb.Feed()
}

func (mb *MasterBaker) Shutdown(reason error) {
	fmt.Printf("%d closing %d->%d (%s)\n", mb.replica.src, mb.state, ConnClosing, reason.Error())
	mb.lock.Lock()
	mb.state = ConnClosing
	mb.err = reason
	mb.lock.Unlock()
}

func (mb *MasterBaker) Feed() (recs toyqueue.Records, err error) {
	switch mb.state {
	case ConnFresh:
		mb.cond.L = &mb.lock
		mb.replica.lock.Lock()
		handshake := toytlv.Record('H',
			toytlv.Record('I', mb.replica.last.ZipBytes()),
			mb.replica.heads.TLV(),
		)
		mb.replica.lock.Unlock()
		recs = append(recs, handshake)
		mb.Advance(ConnHsSent)
		return
	case ConnHsSent:
		mb.lock.Lock()
		fmt.Printf("%d waits\n", mb.replica.src)
		mb.cond.Wait() // wait for their handshake
		fmt.Printf("%d got hs\n", mb.replica.src)
		mb.lock.Unlock()
		return mb.Feed()
	case ConnHsRecvd:
		err = mb.FindLogPosToFeed()
		if err != nil {
			mb.Shutdown(err)
			return nil, nil
		} else {
			mb.Advance(ConnRunning)
			return mb.Feed()
		}
	case ConnRunning:
		recs, err = mb.FeedLog()
		if err != nil {
			mb.Shutdown(err)
		}
		return
	case ConnClosing:
		bye := toytlv.Record('B', []byte(mb.err.Error()))
		mb.Advance(ConnClosed)
		return toyqueue.Records{bye}, nil
	case ConnClosed:
		return nil, nil
	}
	return nil, nil
}

var ErrProtocolViolation = errors.New("protocol violation")
var ErrShutdown = errors.New("bye")

func (mb *MasterBaker) Drain(recs toyqueue.Records) (err error) {
	switch mb.state {
	case ConnFresh, ConnHsSent:
		// parse hs
		rest, empty := toytlv.Take('H', recs[0])
		if rest == nil || len(empty) > 0 {
			return ErrProtocolViolation
		}
		ibody, rest := toytlv.Take('I', rest)
		if ibody == nil || len(rest) == 0 {
			return ErrProtocolViolation
		}
		fmt.Printf("%s connected: %s\n", mb.replica.last.String(), UnzipID(ibody).String())
		vbody, rest := toytlv.TakeRecord('V', rest)
		mb.peervv = make(VV)
		err = mb.peervv.AddTLV(vbody)
		for k, v := range mb.peervv {
			fmt.Printf("%s %x: %x\n", mb.replica.last.String(), k, v)
		}
		if err != nil { // up to this point: no need to shut down gracefully
			return ErrProtocolViolation
		}
		mb.inq = mb.replica.inq.Blocking()
		mb.Advance(ConnHsRecvd)
		recs = recs[1:]
		if len(recs) > 0 {
			return mb.Drain(recs)
		}
		return nil
	case ConnHsRecvd,
		ConnRunning:
		for _, packet := range recs { // FIXME Filter!!!
			lit, body, _ := toytlv.TakeAny(packet)
			if lit == 'B' {
				mb.Shutdown(ErrShutdown)
				return nil
			}
			idbody, _ := toytlv.Take('I', body)
			seqoff, src := UnzipUint64Pair(idbody)
			seq := uint32(seqoff >> OffBits)
			fmt.Printf("%d got %s\n", mb.replica.src, MakeID(uint32(src), seq, 0).String())
			order := mb.peervv.SeeNextSrcSeq(uint32(src), seq) // FIXME no filtering here, but remember ids
			if order == VvSeen {                               // happens normally (e.g. recvd after sent)
				continue
			}
			if order == VvGap { // clearly some problem
				mb.Shutdown(ErrGap)
				return nil
			}
		}
		if err != nil {
			mb.Shutdown(err)
			return nil // gracefully
		}
		err = mb.inq.Drain(recs)
	case ConnClosing:
	case ConnClosed:
		return nil // just ignore
	}
	return
}

var ErrDivergent = errors.New("divergent replicas")

func (mb *MasterBaker) FeedLog() (recs toyqueue.Records, err error) {
	mb.replica.lock.Lock()
	if mb.replica.loglen == mb.loglen {
		mb.replica.drum.Wait()
	}
	chunk, off := mb.replica.FindLogChunk(mb.loglen)
	mb.replica.lock.Unlock()
	var feeder toyqueue.FeedSeekCloser
	feeder = chunk.Feeder()
	if err != nil {
		return
	}
	_, err = feeder.Seek(off, io.SeekStart)
	if err != nil {
		return
	}
	unfiltered, err := feeder.Feed() // fixme read till limit / log sz
	if err != nil {
		return
	}
	dlen := 0
	for _, rec := range unfiltered { // todo filterPackets
		id := PacketID(rec)
		dlen += len(rec)
		if mb.peervv.PutID(id) {
			recs = append(recs, rec)
		}
	}
	mb.loglen += int64(dlen)
	return
}

func (mb *MasterBaker) FindLogPosToFeed() (err error) {
	c := -1
	chunk, pos := mb.replica.GetLogChunk(c)
	for chunk != nil {
		var feeder toyqueue.FeedSeekCloser
		feeder = chunk.Feeder()
		if err != nil {
			return
		}
		var recs toyqueue.Records
		recs, err = feeder.Feed()
		vv := make(VV)
		err = vv.AddTLV(recs[0])
		if err != nil {
			return
		}
		pos += int64(len(recs[0]))
		if mb.peervv.Seen(vv) {
			break
		}
		c--
		chunk, pos = mb.replica.GetLogChunk(c)
	}
	if chunk == nil {
		err = ErrDivergent
	} else {
		mb.loglen = pos
	}
	return
}

func (mb *MasterBaker) Close() error {
	// todo???
	return nil
}
