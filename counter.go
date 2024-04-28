package chotki

import (
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/protocol"
	"github.com/drpcorg/chotki/utils"
)

type Counter64 int64

func (c *Counter64) Apply(state []byte) {
	sum, _ := parseC(state, 0)
	*c = Counter64(sum)
}

func (c Counter64) Diff(id rdx.ID, state []byte) (changes []byte) {
	sum, mine := parseC(state, id.Src())
	if sum != int64(c) {
		d := int64(c) - sum
		new_own := mine + d
		changes = protocol.Concat(
			protocol.Record('I', id.ZipBytes()),
			protocol.Record('C', rdx.ZipUint64(rdx.ZigZagInt64(new_own))),
		)
	}
	return
}

func ProbeID(lit byte, input []byte) rdx.ID {
	idbody, _ := protocol.Take(lit, input)
	return rdx.IDFromZipBytes(idbody)
}

func ProbeI(input []byte) rdx.ID {
	return ProbeID('I', input)
}

// I id C int
func CMerge(inputs [][]byte) (merged []byte) {
	heap := utils.Heap[uint64]{}
	for i, in := range inputs { // fixme 4096
		id := ProbeI(in)
		reid := rdx.IDFromSrcSeqOff(id.Src(), id.Seq(), uint16(i)) // todo i order
		heap.Push(^uint64(reid))
	}
	prev := uint64(0)
	for heap.Len() > 0 {
		id := rdx.ID(^heap.Pop())
		i := int(id.Off())
		iclen := protocol.ProbeHeaders("IC", inputs[i])
		if iclen == -1 {
			continue //?!
		}
		if id.Src() != prev {
			merged = append(merged, inputs[i][:iclen]...)
			prev = id.Src()
		}
		inputs[i] = inputs[i][iclen:]
		if len(inputs[i]) != 0 {
			id := ProbeI(inputs[i])
			reid := rdx.IDFromSrcSeqOff(id.Src(), id.Seq(), uint16(i)) // todo i order
			heap.Push(^uint64(reid))
		}
	}
	return
}

func CState(sum int64) []byte {
	return protocol.Record('C', rdx.ZipZagInt64(sum))
}

func parseC(state []byte, src uint64) (sum, mine int64) {
	rest := state
	var err error
	var id rdx.ID
	var cbody []byte
	for len(rest) > 0 {
		id, rest, err = rdx.TakeIDWary('I', rest)
		if err != nil {
			return
		}
		cbody, rest = protocol.Take('C', rest)
		inc := rdx.ZagZigUint64(rdx.UnzipUint64(cbody))
		if id.Src() == src {
			mine = inc
		}
		sum += inc
	}
	return
}
