package rdx

import (
	"errors"
	"github.com/learn-decentralized-systems/toyqueue"
)

const (
	RdxNone = iota
	RdxFloat
	RdxInt
	RdxRef
	RdxString
	RdxTomb
	RdxMap
	RdxSet
	RdxArray
	RdxName
	RdxObject
	RdxPath
)

type RDX struct {
	RdxType int
	Parent  *RDX
	Nested  []RDX
	Text    []byte
}

const RdxMaxNesting = 64

var RdxSep = []byte("{}[],:.")

const (
	RdxOOpen = iota
	RdxOClose
	RdxAOpen
	RdxAClose
	RdxComma
	RdxColon
	RdxDot
)

var ErrBadRdx = errors.New("bad RDX syntax")

func (rdx *RDX) AddChild(rdxtype int, text []byte) {
	rdx.Nested = append(rdx.Nested, RDX{
		RdxType: rdxtype,
		Text:    text,
	})
}

func (rdx *RDX) String() string {
	recs, _ := rdx.Feed()
	var by []byte
	for _, rec := range recs {
		by = append(by, rec...)
	}
	return string(by)
}

func (rdx *RDX) Feed() (recs toyqueue.Records, err error) {
	switch rdx.RdxType {
	case RdxNone:
	case RdxFloat:
		recs = append(recs, rdx.Text)
	case RdxInt:
		recs = append(recs, rdx.Text)
	case RdxRef:
		recs = append(recs, rdx.Text)
	case RdxString:
		recs = append(recs, rdx.Text)
	case RdxTomb:
		recs = append(recs, rdx.Text)
	case RdxMap:
		recs = append(recs, RdxSep[RdxOOpen:RdxOOpen+1])
		for i := 0; i+1 < len(rdx.Nested); i += 2 {
			key, _ := rdx.Nested[i].Feed()
			val, _ := rdx.Nested[i+1].Feed()
			recs = append(recs, key...)
			recs = append(recs, RdxSep[RdxColon:RdxColon+1])
			recs = append(recs, val...)
			if i+2 < len(rdx.Nested) {
				recs = append(recs, RdxSep[RdxComma:RdxComma+1])
			}
		}
		recs = append(recs, RdxSep[RdxOClose:RdxOClose+1])
	case RdxSet:
	case RdxArray:
	case RdxName:
		recs = append(recs, rdx.Text)
	case RdxObject:
	case RdxPath:
		for i := 0; i < len(rdx.Nested); i++ {
			t, _ := rdx.Nested[i].Feed()
			recs = append(recs, t...)
			if i+1 < len(rdx.Nested) {
				recs = append(recs, RdxSep[RdxDot:RdxDot+1])
			}
		}
	}
	return
}
