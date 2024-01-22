package main

import (
	"github.com/learn-decentralized-systems/toytlv"
)

func LMerge(inputs [][]byte) []byte {
	var maxt int64 = -1
	var win []byte
	for _, in := range inputs {
		for len(in) > 0 {
			ilit, ihdrlen, ibdylen := toytlv.ProbeHeader(in)
			if ilit != 'I' {
				in = nil
				break
			}
			xrest := in[ihdrlen+ibdylen:]
			_, xhdrlen, xbdylen := toytlv.ProbeHeader(xrest)
			ixlen := ihdrlen + ibdylen + xhdrlen + xbdylen
			rest := in[ixlen:]
			xbody := xrest[xhdrlen : xhdrlen+xbdylen]
			tlit, thdrlen, tbdylen := toytlv.ProbeHeader(xbody)
			if tlit != 'T' {
				in = rest
				continue
			}
			tzip := xbody[thdrlen : thdrlen+tbdylen]
			t := int64(UnzipUint64(tzip))
			if t == maxt {
				// todo xbody
			}
			if t > maxt {
				maxt = t
				win = in[:ixlen]
			}
			in = rest
		}
	}
	return win
}
