package main

import (
	"errors"
	"fmt"
	"github.com/learn-decentralized-systems/toylog"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"io"
	"sync"
)

const (
	ConnFresh = iota
	ConnHsSent
	ConnHsRecvd
	ConnReSync
	ConnRunning
	ConnClosing
	ConnClosed
)

type MasterBaker struct {
	state   int
	replica *Chotki
	peervv  VV
	ketchup toyqueue.FeedCloser
	outq    toyqueue.FeedDrainCloser
	lock    sync.Mutex
	cond    sync.Cond
	inq     toyqueue.Drainer
	err     error
}

func (ms *MasterBaker) Advance(new_state int) {
	fmt.Printf("%d state %d->%d\n", ms.replica.src, ms.state, new_state)
	ms.lock.Lock()
	ms.state = new_state
	ms.cond.Broadcast()
	ms.lock.Unlock()
}

func (ms *MasterBaker) Forward(new_state int) (recs toyqueue.Records, err error) {
	ms.Advance(new_state)
	return ms.Feed()
}

func (ms *MasterBaker) Shutdown(reason error) {
	fmt.Printf("%d closing %d->%d (%s)\n", ms.replica.src, ms.state, ConnClosing, reason.Error())
	ms.lock.Lock()
	ms.state = ConnClosing
	ms.err = reason
	ms.lock.Unlock()
}

func (ms *MasterBaker) Feed() (recs toyqueue.Records, err error) {
	switch ms.state {
	case ConnFresh:
		ms.cond.L = &ms.lock
		ms.replica.lock.Lock()
		handshake := toytlv.Record('H',
			toytlv.Record('I', ms.replica.NewID().ZipBytes()),
			ms.replica.heads.Bytes(),
		)
		recs = append(recs, handshake)
		ms.replica.lock.Unlock()
		ms.Advance(ConnHsSent)
		return
	case ConnHsSent:
		ms.lock.Lock()
		fmt.Printf("%d waits\n", ms.replica.src)
		ms.cond.Wait() // wait for their handshake
		fmt.Printf("%d got hs\n", ms.replica.src)
		ms.lock.Unlock()
		return ms.Feed()
	case ConnHsRecvd:
		recs, err = ms.OpenLogForCatchUp()
		if err != nil {
			ms.Shutdown(err)
			return nil, nil
		} else if len(recs) > 0 {
			ms.Advance(ConnReSync)
			return recs, nil
		} else {
			ms.Advance(ConnReSync)
			return ms.Feed()
		}
	case ConnReSync:
		for len(recs) == 0 && err == nil {
			recs, err = ms.ketchup.Feed()
			recs, _ = ms.peervv.Filter(recs) // todo gap etc
		}
		if err == io.EOF { // caught up
			ms.Advance(ConnRunning)
			ms.replica.lock.Lock()
			// outq fanout happens after fsynced write...
			outq := toyqueue.RecordQueue{
				Limit: 1024,
			}
			ms.outq = outq.Blocking()
			ms.replica.outq = append(ms.replica.outq, ms.outq)
			ms.replica.lock.Unlock()
			// ...so we check for concurrent writes here
			recs, _ = ms.ketchup.Feed()
			recs, err = ms.peervv.Filter(recs)
			if err != nil {
				ms.Shutdown(err)
				recs = nil
			}
			if len(recs) == 0 {
				return ms.Feed()
			}
		} else if err != nil {
			ms.Shutdown(err)
			return ms.Feed()
		} else {
			return
		}
	case ConnRunning:
		for len(recs) == 0 && err == nil {
			recs, err = ms.outq.Feed()
			if err == nil {
				recs, err = ms.peervv.Filter(recs)
			}
		}
		if err != nil { // e.g. toyqueue.ErrClosed
			ms.Shutdown(err)
			return ms.Feed()
		}
		return
	case ConnClosing:
		bye := toytlv.Record('B', []byte(ms.err.Error()))
		ms.Advance(ConnClosed)
		return toyqueue.Records{bye}, nil
	case ConnClosed:
		return nil, nil
	}
	return nil, nil
}

var ErrProtocolViolation = errors.New("protocol violation")
var ErrShutdown = errors.New("bye")

func (ms *MasterBaker) Drain(recs toyqueue.Records) (err error) {
	switch ms.state {
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
		fmt.Printf("%s connected: %s\n", ms.replica.last.String(), UnzipID(ibody).String())
		vbody, rest := toytlv.TakeRecord('V', rest)
		ms.peervv = make(VV)
		err = ms.peervv.LoadBytes(vbody)
		for k, v := range ms.peervv {
			fmt.Printf("%s %x: %x\n", ms.replica.last.String(), k, v)
		}
		if err != nil { // up to this point: no need to shut down gracefully
			return ErrProtocolViolation
		}
		ms.inq = ms.replica.inq.Blocking()
		ms.Advance(ConnHsRecvd)
		recs = recs[1:]
		if len(recs) > 0 {
			return ms.Drain(recs)
		}
		return nil
	case ConnHsRecvd,
		ConnReSync,
		ConnRunning:
		// todo err state switches
		for _, packet := range recs { // FIXME Filter!!!
			lit, body, _ := toytlv.TakeAny(packet)
			if lit == 'B' {
				ms.Shutdown(ErrShutdown)
				return nil
			}
			idbody, _ := toytlv.Take('I', body)
			seqoff, src := UnzipUint32Pair(idbody)
			fmt.Printf("%d got %s\n", ms.replica.src, MakeID(src, seqoff>>OffBits, 0).String())
			order := ms.peervv.Next2(seqoff>>OffBits, src)
			if order == VvSeen { // happens normally (e.g. recvd after sent)
				continue
			}
			if order == VvGap { // clearly some problem
				ms.Shutdown(ErrGap)
				return nil
			}
		}
		if err != nil {
			ms.Shutdown(err)
			return nil // gracefully
		}
		err = ms.inq.Drain(recs)
	case ConnClosing:
	case ConnClosed:
		return nil // just ignore
	}
	return
}

var ErrDivergent = errors.New("divergent replicas")

func (ms *MasterBaker) OpenLogForCatchUp() (recs toyqueue.Records, err error) {
	c := int64(0)
	reader, err := ms.replica.log.Reader(c, toylog.ChunkSeekEnd)
	if err == nil {
		_, err = reader.Seek(c, toylog.ChunkSeekEnd)
	}
	if err != nil {
		return nil, err
	}
	feeder := toytlv.FeedSeekCloser{Reader: reader}
	for {
		recs, err = feeder.Feed()
		vv := make(VV)
		err = vv.LoadBytes(recs[0])
		recs = recs[1:]
		if err != nil {
			return nil, ErrBadVRecord
		}
		if ms.peervv.Covers(vv) {
			break
		}
		c++
		_, err = feeder.Seek(c, toylog.ChunkSeekEnd)
		if err != nil {
			return nil, ErrDivergent
		}
	}

	ms.ketchup = &feeder

	return recs, nil
}

func (mb *MasterBaker) Close() error {
	// todo???
	return nil
}
