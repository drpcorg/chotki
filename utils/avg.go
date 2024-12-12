package utils

import "sync"

type AvgVal struct {
	v     float64
	count int
	lock  sync.Mutex
}

func NewAvgVal(val float64) *AvgVal {
	return &AvgVal{
		v:     val,
		count: 1,
	}
}

func (a *AvgVal) Add(val float64) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.v = (float64(a.count)*a.v + val) / float64(a.count+1)
	a.count++
}

func (a *AvgVal) Val() float64 {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.v
}
