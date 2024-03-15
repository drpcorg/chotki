package main

import (
	"github.com/learn-decentralized-systems/toytlv"
)

type counter64 int64

func ProbeID(lit byte, input []byte) ID {
	idbody, _ := toytlv.Take(lit, input)
	return IDFromZipBytes(idbody)
}

func ProbeI(input []byte) ID {
	return ProbeID('I', input)
}

// I id C int
func CMerge(inputs [][]byte) (merged []byte) {
	var _heap [32]uint64
	heap := Uint64Heap(_heap[0:0])
	for i, in := range inputs { // fixme 4096
		id := ProbeI(in)
		reid := IDFromSrcSeqOff(id.Src(), id.Seq(), uint16(i)) // todo i order
		heap.Push(^uint64(reid))
	}
	prev := uint64(0)
	for len(heap) > 0 {
		id := ID(^heap.Pop())
		//fmt.Printf("picked %s\n", id.String())
		i := int(id.Off())
		iclen := toytlv.ProbeHeaders("IC", inputs[i])
		if iclen == -1 {
			continue //?!
		}
		if id.Src() != prev {
			//fmt.Println("accepted")
			merged = append(merged, inputs[i][:iclen]...)
			prev = id.Src()
		}
		inputs[i] = inputs[i][iclen:]
		if len(inputs[i]) != 0 {
			id := ProbeI(inputs[i])
			//fmt.Printf("queued %s\n", id.String())
			reid := IDFromSrcSeqOff(id.Src(), id.Seq(), uint16(i)) // todo i order
			heap.Push(^uint64(reid))
		}
	}
	return
}

func CState(sum int64) []byte {
	return toytlv.Record('C', ZipZagInt64(sum))
}

func parseC(state []byte, src uint64) (sum, mine int64) {
	rest := state
	var err error
	var id ID
	var cbody []byte
	for len(rest) > 0 {
		id, rest, err = TakeIDWary('I', rest)
		if err != nil {
			return
		}
		cbody, rest = toytlv.Take('C', rest)
		inc := ZagZigUint64(UnzipUint64(cbody))
		if id.Src() == src {
			mine = inc
		}
		sum += inc
	}
	return
}

func (c *counter64) Apply(state []byte) {
	sum, _ := parseC(state, 0)
	*c = counter64(sum)
}

func (c counter64) Diff(id ID, state []byte) (changes []byte) {
	sum, mine := parseC(state, id.Src())
	if sum != int64(c) {
		d := int64(c) - sum
		new_own := mine + d
		changes = toytlv.Concat(
			toytlv.Record('I', id.ZipBytes()),
			toytlv.Record('C', ZipUint64(ZigZagInt64(new_own))),
		)
	}
	return
}
