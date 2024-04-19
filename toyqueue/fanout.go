package toyqueue

import (
	"errors"
	"sync"
)

type multiDrain struct {
	drains []DrainCloser
	lock   sync.Mutex
}

type Fanout struct {
	multiDrain
	feeder FeedCloser
}

func FanoutFeeder(feeder FeedCloser) *Fanout {
	return &Fanout{
		feeder: feeder,
	}
}

func FanoutQueue(limit int) (fanout *Fanout, queue DrainCloser) {
	q := RecordQueue{Limit: limit}
	fanout = &Fanout{feeder: &q}
	queue = &q
	return
}

func (f2ds *multiDrain) AddDrain(drain DrainCloser) {
	f2ds.lock.Lock()
	f2ds.drains = append(f2ds.drains, drain)
	f2ds.lock.Unlock()
}

var ErrNotKnown = errors.New("unknown drain")

func (f2ds *multiDrain) findDrain(drain DrainCloser) int {
	i := 0
	l := len(f2ds.drains)
	for i < l && f2ds.drains[i] != drain {
		i++
	}
	return i
}

func (f2ds *multiDrain) RemoveDrain(drain DrainCloser) (err error) {
	f2ds.lock.Lock()
	l := len(f2ds.drains)
	i := f2ds.findDrain(drain)
	if i < l {
		f2ds.drains[i] = f2ds.drains[l-1]
		f2ds.drains = f2ds.drains[:l-1]
	} else {
		err = ErrNotKnown
	}
	f2ds.lock.Unlock()
	return
}

func (f2ds *multiDrain) HasDrain(drain DrainCloser) (has bool) {
	f2ds.lock.Lock()
	has = f2ds.findDrain(drain) < len(f2ds.drains)
	f2ds.lock.Unlock()
	return
}

// Run shovels the data from the feeder to the drains.
func (f2ds *Fanout) Run() {
	var ferr, derr error
	for ferr == nil && derr == nil {
		var recs Records
		recs, ferr = f2ds.feeder.Feed()
		if len(recs) > 0 {
			f2ds.lock.Lock()
			ds := f2ds.drains
			f2ds.lock.Unlock()
			for i := 0; i < len(ds) && derr == nil; i++ {
				derr = ds[i].Drain(recs)
			}
		}
	}
	_ = f2ds.feeder.Close()
	f2ds.lock.Lock()
	ds := f2ds.drains
	f2ds.drains = nil
	f2ds.feeder = nil
	f2ds.lock.Unlock()
	for _, drain := range ds {
		_ = drain.Close()
	}
}

type feederDrainer struct {
	feed  Feeder
	drain Drainer
}

func (fd *feederDrainer) Feed() (recs Records, err error) {
	return fd.feed.Feed()
}

func (fd *feederDrainer) Drain(recs Records) error {
	return fd.drain.Drain(recs)
}

func JoinedFeedDrainer(feeder Feeder, drainer Drainer) FeedDrainer {
	return &feederDrainer{feed: feeder, drain: drainer}
}
