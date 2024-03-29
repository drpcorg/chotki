package rdx

import (
	"testing"
)

func TestVMerge(t *testing.T) {
	/*
		args := [][]byte{
			toytlv.Concat(
				toytlv.Record('V', ParseIDString("a-123").ZipBytes()),
				toytlv.Record('V', ParseIDString("b-345").ZipBytes()),
			),
			toytlv.Concat(
				toytlv.Record('V', ParseIDString("a-234").ZipBytes()),
				toytlv.Record('V', ParseIDString("b-344").ZipBytes()),
				toytlv.Record('V', ParseIDString("c-567").ZipBytes()),
			),
		}
		//result := Vmerge(args)
			ma := chotki.PebbleMergeAdaptor{
				id:  ID0,
				rdt: 'V',
			}
			_ = ma.MergeNewer(args[0])
			_ = ma.MergeNewer(args[1])
			actual, _, _ := ma.Finish(true)
			correct := toytlv.Concat(
				toytlv.Record('V', ParseIDString("a-234").ZipBytes()),
				toytlv.Record('V', ParseIDString("b-345").ZipBytes()),
				toytlv.Record('V', ParseIDString("c-567").ZipBytes()),
			)
			//ac := Vplain(actual)
			//fmt.Println(ac.String())
			assert.Equal(t, correct, actual)
		    FIXME */
}
