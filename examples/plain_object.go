package examples

import (
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
)

// todo RDX formula

type Student struct {
	Name string

	Group rdx.ID

	Score uint64
}

var StudentClassId = rdx.IDFromString("1f-2")

func (o *Student) Load(off uint64, rdt byte, tlv []byte) error {
	switch off {

	case 1:

		if rdt != 'S' {
			break
		}
		o.Name = rdx.Snative(tlv)

	case 2:

		if rdt != 'R' {
			break
		}
		o.Group = rdx.Rnative(tlv)

	case 3:

		if rdt != 'N' {
			break
		}
		o.Score = rdx.Nnative(tlv)

	default:
		return chotki.ErrUnknownFieldInAType
	}
	return nil
}

func (o *Student) Store(off uint64, rdt byte, old []byte) (bare []byte, err error) {
	switch off {

	case 1:

		if rdt != 'S' {
			break
		}
		if old == nil {
			bare = rdx.Stlv(o.Name)
		} else {
			bare = rdx.Sdelta(old, o.Name)
		}

	case 2:

		if rdt != 'R' {
			break
		}
		if old == nil {
			bare = rdx.Rtlv(o.Group)
		} else {
			bare = rdx.Rdelta(old, o.Group)
		}

	case 3:

		if rdt != 'N' {
			break
		}
		if old == nil {
			bare = rdx.Ntlv(o.Score)
		} else {
			bare = rdx.Ndelta(old, o.Score)
		}

	default:
		return nil, chotki.ErrUnknownFieldInAType
	}
	return
}
