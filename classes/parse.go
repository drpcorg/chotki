package classes

import "github.com/drpcorg/chotki/rdx"

func ParseClass(tlv []byte) (fields Fields) {
	it := rdx.FIRSTIterator{TLV: tlv}
	fields = append(fields, Field{ // todo inheritance
		Offset:  0,
		Name:    "_ref",
		RdxType: rdx.Reference,
	})
	for it.Next() {
		lit, t, name := it.ParsedValue()
		if lit != rdx.Term || len(name) == 0 {
			break // todo unique names etc
		}
		rdt := rdx.String
		index := IndexType(0)
		if name[0] >= 'A' && name[0] <= 'Z' {
			rdt = name[0]
			index = IndexType(name[1])
			name = name[2:]
		}
		fields = append(fields, Field{
			Offset:  t.Rev,
			RdxType: rdt,
			Name:    string(name),
			Index:   index,
		})
	}
	return
}
