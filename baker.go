package main

import (
	"errors"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"net"
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
	logrdr  toyqueue.FeederCloser
	outq    toyqueue.RecordQueue
	lock    sync.Mutex
	cond    sync.Cond
	inq     toyqueue.Drainer
	err     error
}

func (ms *MasterBaker) Advance(new_state int) {
	ms.lock.Lock()
	ms.state = new_state
	ms.lock.Unlock()
}

func (ms *MasterBaker) Feed() (recs toyqueue.Records, err error) {
	switch ms.state {
	case ConnFresh: // send my vv
		ms.replica.lock.Lock()
		recs = append(recs, ms.replica.heads.Bytes())
		ms.replica.lock.Unlock()
	case ConnHsSent:
		ms.lock.Lock()
		ms.cond.Wait()
		ms.lock.Unlock()
		return ms.Feed()
	case ConnHsRecvd:
		// init log reader
		ms.Advance(ConnReSync)
		return ms.Feed()
	case ConnReSync:
		recs, err = ms.logrdr.Feed()
		// todo filter
		if len(recs) == 0 { // EOF
			ms.Advance(ConnRunning)
			// add outq
			ms.replica.outq = append(ms.replica.outq, &ms.outq)
			recs, err = ms.logrdr.Feed() // retry
			if len(recs) > 0 {
				return recs, err
			} else {
				return ms.Feed()
			}
		}
	case ConnRunning:
		recs, err = ms.outq.Feed()
		// todo filter
		if toytlv.Lit(recs[len(recs)-1]) == 'F' {
			ms.Advance(ConnClosing)
		}
	case ConnClosing:
		ms.Advance(ConnClosed)
		return nil, nil
	case ConnClosed:
		panic("TCP depot ignored EOF")
	}
	return nil, nil
}

var ErrProtocolViolation = errors.New("protocol violation")

func (ms *MasterBaker) Drain(recs toyqueue.Records) (err error) {
	switch ms.state {
	case ConnFresh, ConnHsSent:
		lit := toytlv.Lit(recs[0])
		if lit != 'V' {
			return ErrProtocolViolation
		}
		ms.peervv = make(VV) // todo
		err = ms.peervv.LoadBytes(recs[0])
		if err == nil {
			ms.Advance(ConnHsRecvd) // FIXME
			ms.inq = ms.replica.inq.Blocking()
		}
	case ConnHsRecvd,
		ConnReSync,
		ConnRunning:
		var ops Batch
		ops, err = ms.peervv.Filter(Batch(recs))
		if err == nil {
			err = ms.replica.inq.Drain(toyqueue.Records(ops))
		}
	case ConnClosing:
	case ConnClosed:
		return net.ErrClosed
	}
	return
}

func (mb *MasterBaker) Close() error {
	// todo
	return nil
}
