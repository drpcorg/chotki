package rdx

import (
	"errors"

	"github.com/drpcorg/chotki/protocol"
)

const (
	None      = byte(0)
	Float     = byte('F')
	Integer   = byte('I')
	Reference = byte('R')
	String    = byte('S')
	Term      = byte('T')
	Natural   = byte('N')
	NInc      = byte('n')
	ZCounter  = byte('Z')
	ZInc      = byte('z')
	Eulerian  = byte('E')
	Linear    = byte('L')
	Mapping   = byte('M')
	VVector   = byte('V')
)

type RDX struct {
	Nested  []RDX
	Text    []byte
	Parent  *RDX
	RdxType byte
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
var ErrBadValueForAType = errors.New("rdx: bad value for the type")

func (rdx *RDX) AddChild(rdxtype byte, text []byte) {
	rdx.Nested = append(rdx.Nested, RDX{
		RdxType: rdxtype,
		Text:    text,
	})
}

func (rdx *RDX) FIRST() bool {
	return rdx != nil && (rdx.RdxType == Float || rdx.RdxType == Integer ||
		rdx.RdxType == Reference || rdx.RdxType == String || rdx.RdxType == Term)
}

func (rdx *RDX) String() string {
	recs, _ := rdx.Feed()
	var by []byte
	for _, rec := range recs {
		by = append(by, rec...)
	}
	return string(by)
}

func (rdx *RDX) Feed() (recs protocol.Records, err error) {
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
	case Term:
		recs = append(recs, rdx.Text)
	case Natural, NInc, ZCounter, ZInc:
		recs = append(recs, rdx.Text)
	case Mapping:
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
	case Eulerian:
		recs = append(recs, RdxSep[RdxOOpen:RdxOOpen+1])
		for i := 0; i < len(rdx.Nested); i++ {
			val, _ := rdx.Nested[i].Feed()
			recs = append(recs, val...)
			if i+1 < len(rdx.Nested) {
				recs = append(recs, RdxSep[RdxComma:RdxComma+1])
			}
		}
		recs = append(recs, RdxSep[RdxOClose:RdxOClose+1])
	case Linear:
		recs = append(recs, RdxSep[RdxAOpen:RdxAOpen+1])
		for i := 0; i < len(rdx.Nested); i++ {
			val, _ := rdx.Nested[i].Feed()
			recs = append(recs, val...)
			if i+1 < len(rdx.Nested) {
				recs = append(recs, RdxSep[RdxComma:RdxComma+1])
			}
		}
		recs = append(recs, RdxSep[RdxAClose:RdxAClose+1])
	}
	return
}

//go:generate ragel -Z -o rdx.ragel.go rdx.rl
