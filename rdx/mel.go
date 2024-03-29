package rdx

import (
	"errors"
	"github.com/learn-decentralized-systems/toytlv"
)

type Time struct {
	rev int64
	src uint64
}

func (t Time) ZipBytes() []byte {
	return ZipIntUint64Pair(t.rev, t.src)
}

func TimeFromZipBytes(zip []byte) (t Time) {
	// todo bad data
	t.rev, t.src = UnzipIntUint64Pair(zip)
	return
}

var ErrBadISFR = errors.New("bad ISFR record")

// Parses an enveloped ISFR record
func MelParse(data []byte) (lit byte, t Time, value, rest []byte, err error) {
	var hlen, blen int
	lit, hlen, blen = toytlv.ProbeHeader(data)
	if lit == 0 || hlen+blen > len(data) {
		err = toytlv.ErrIncomplete
		return
	}
	rec := data[:hlen+blen]
	rest = data[hlen+blen:]
	tlit, thlen, tblen := toytlv.ProbeHeader(data)
	tlen := thlen + tblen
	if (tlit != 'T' && tlit != '0') || (tlen > len(rec)) {
		err = ErrBadISFR
		return
	}
	tsb := rec[thlen:tlen]
	t.rev, t.src = UnzipIntUint64Pair(tsb)
	value = rec[tlen:]
	return
}

func MelAppend(to []byte, lit byte, t Time, body []byte) []byte {
	tb := toytlv.TinyRecord('T', t.ZipBytes())
	return toytlv.Append(to, lit, tb, body)
}

func MelReSource(isfr []byte, src uint64) (ret []byte, err error) {
	var lit byte
	var time Time
	var body []byte
	rest := isfr
	for len(rest) > 0 {
		at := len(isfr) - len(rest)
		lit, time, body, rest, err = MelParse(rest)
		if err != nil {
			return
		}
		if time.src != src {
			ret = make([]byte, at, len(isfr)*2)
			copy(ret, isfr[:at])
			break
		}
	}
	if ret == nil && err == nil {
		return isfr, nil
	}
	for err == nil {
		time.src = src
		ret = MelAppend(ret, lit, time, body)
		if len(rest) == 0 {
			break
		}
		lit, time, body, rest, err = MelParse(rest)
	}
	return
}
