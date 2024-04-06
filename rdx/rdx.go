package rdx

import (
	"errors"
	"github.com/learn-decentralized-systems/toyqueue"
)

const (
	None = iota
	Float
	Integer
	Reference
	String
	Tomb
	NCounter
	NInc
	ZCounter
	ZInc
	ESet
	LArray
	Map
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
	case None:
	case Float:
		recs = append(recs, rdx.Text)
	case Integer:
		recs = append(recs, rdx.Text)
	case Reference:
		recs = append(recs, rdx.Text)
	case String:
		recs = append(recs, rdx.Text)
	case Tomb:
		recs = append(recs, rdx.Text)
	case NCounter, NInc, ZCounter, ZInc:
		recs = append(recs, rdx.Text)
	case Map:
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
	case ESet:
		recs = append(recs, RdxSep[RdxOOpen:RdxOOpen+1])
		for i := 0; i < len(rdx.Nested); i++ {
			val, _ := rdx.Nested[i].Feed()
			recs = append(recs, val...)
			if i+1 < len(rdx.Nested) {
				recs = append(recs, RdxSep[RdxComma:RdxComma+1])
			}
		}
		recs = append(recs, RdxSep[RdxOClose:RdxOClose+1])
	case LArray:
		recs = append(recs, RdxSep[RdxAOpen:RdxAOpen+1])
		for i := 0; i < len(rdx.Nested); i++ {
			val, _ := rdx.Nested[i].Feed()
			recs = append(recs, val...)
			if i+1 < len(rdx.Nested) {
				recs = append(recs, RdxSep[RdxComma:RdxComma+1])
			}
		}
		recs = append(recs, RdxSep[RdxAClose:RdxAClose+1])
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

//go:generate ragel-go -o cmd.ragel.go cmd.rl
//go:generate ragel-go -o rdx.ragel.go rdx.rl
