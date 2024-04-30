package rdx

type Clock interface {
	See(time, src uint64)
	Time(maxTime uint64) uint64
	Src() uint64
}

type LocalLogicalClock struct {
	Source uint64
}

func (llc *LocalLogicalClock) See(time, src uint64) {
}

func (llc *LocalLogicalClock) Time(maxtime uint64) uint64 {
	return maxtime + 1
}

func (llc *LocalLogicalClock) Src() uint64 {
	return llc.Source
}
