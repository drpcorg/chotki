package toyqueue

type twoWayQueue struct {
	in  DrainCloser
	out FeedCloser
}

func RecordQueuePair(limit int) (i, o FeedDrainCloser) {
	a := RecordQueue{Limit: limit}
	b := RecordQueue{Limit: limit}
	i = &twoWayQueue{in: &a, out: &b}
	o = &twoWayQueue{in: &b, out: &a}
	return
}

func BlockingRecordQueuePair(limit int) (i, o FeedDrainCloser) {
	_a, _b := RecordQueue{Limit: limit}, RecordQueue{Limit: limit}
	a, b := _a.Blocking(), _b.Blocking()
	i = &twoWayQueue{in: a, out: b}
	o = &twoWayQueue{in: b, out: a}
	return
}

func (tw *twoWayQueue) Feed() (recs Records, err error) {
	return tw.out.Feed()
}

func (tw *twoWayQueue) Drain(recs Records) error {
	return tw.in.Drain(recs)
}

func (tw *twoWayQueue) Close() (err error) {
	err = tw.in.Close()
	if err == nil {
		err = tw.out.Close()
	}
	return
}
