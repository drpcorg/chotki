package chotki

import (
	"github.com/drpcorg/chotki/rdx"
	"github.com/drpcorg/chotki/toytlv"
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
		changes = toytlv.Concat(
			toytlv.Record('I', id.ZipBytes()),
			toytlv.Record('C', rdx.ZipUint64(rdx.ZigZagInt64(new_own))),
		)
	}
	return
}

func ProbeID(lit byte, input []byte) rdx.ID {
	idbody, _ := toytlv.Take(lit, input)
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
			reid := rdx.IDFromSrcSeqOff(id.Src(), id.Seq(), uint16(i)) // todo i order
			heap.Push(^uint64(reid))
		}
	}
	return
}

func CState(sum int64) []byte {
	return toytlv.Record('C', rdx.ZipZagInt64(sum))
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
		cbody, rest = toytlv.Take('C', rest)
		inc := rdx.ZagZigUint64(rdx.UnzipUint64(cbody))
		if id.Src() == src {
			mine = inc
		}
		sum += inc
	}
	return
}
