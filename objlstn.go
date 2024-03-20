package main

import (
	"github.com/learn-decentralized-systems/toyqueue"
)

type FieldTrigger func(id ID, state []byte)

type ObjectListener struct {
	inq   toyqueue.FeedCloser
	lstns map[ID][]*FieldTrigger
}

func (ol *ObjectListener) AddTrigger(id ID, trigger *FieldTrigger) {
	triggers := ol.lstns[id]
	triggers = append(triggers, trigger)
	ol.lstns[id] = triggers
}

func (ol *ObjectListener) RemoveTrigger(id ID, trigger *FieldTrigger) {
	triggers := ol.lstns[id]
	for n := 0; n < len(triggers); n++ {
		if triggers[n] == trigger {
			triggers[n] = triggers[len(triggers)-1]
			triggers = triggers[:len(triggers)-1]
		}
	}
	ol.lstns[id] = triggers
}

func (ol *ObjectListener) DoListen() {
	recs, err := ol.inq.Feed()
	for err == nil {
		for _, rec := range recs {
			id := PacketID(rec)
			lstn := ol.lstns[id]
			for _, l := range lstn {
				(*l)(id, rec)
			}
		}
		recs, err = ol.inq.Feed()
	}
}
