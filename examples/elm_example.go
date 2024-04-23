package examples

import (
	"github.com/cockroachdb/pebble"
	"github.com/drpcorg/chotki"
	"github.com/drpcorg/chotki/rdx"
)

type Group struct {
	host *chotki.ORM
	Name string
}

type Student struct {
	host  *chotki.ORM
	Name  string
	Score int64 // todo counter
	group rdx.ID
}

func (group *Group) Load(i *pebble.Iterator) error {
	return nil
	// todo switch, off + rdt

}

func (group *Group) Store(i *pebble.Iterator) (changes [][]byte, err error) {
	return nil, nil

}

// RDX
func (group *Group) String() string {
	return ""
}

func (group *Group) JSON() string {
	return ""

}

func (stud *Student) Load(i *pebble.Iterator) error {
	return nil

}

func (stud *Student) Store(i *pebble.Iterator) (changes [][]byte, err error) {
	return nil, nil

}

func (stud *Student) Group() (group *Group) {
	/*if stud.group == rdx.ID0 {
		return nil
	}
	var blank Group
	obj, err := stud.host.Load(stud.group, &blank)
	if err != nil {
		return nil // ?
	}
	ret, ok := obj.(*Group)
	if !ok {
		return nil
	}
	return ret*/
	return nil
}

type Course struct {
	Title    string
	Summary  string
	Students map[rdx.ID]*Student
	Marks    map[rdx.ID]int64
}

type SomeClass struct {
	Name string
	Age  int64
}

var SomeClassClassID = rdx.ID(49733109947568128)

func (o *SomeClass) Load(i *pebble.Iterator) (err error) {
	for ; i.Valid(); i.Next() {
		id, rdt := chotki.OKeyIdRdt(i.Key())
		switch id.Off() {
		case 0:
			if rdt != 'O' {
				return chotki.ErrBadORecord
			}
			z := rdx.IDFromZipBytes(i.Value())
			if z != SomeClassClassID {
				return chotki.ErrBadORecord
			}
		case 1:
			if rdt != 'S' {
				break
			}
			o.Name = rdx.Snative(i.Value())
		case 2:
			if rdt != 'I' {
				break
			}
			o.Age = rdx.Inative(i.Value())
		}
	}
	return nil
}
func (o *SomeClass) Store(i *pebble.Iterator) (delta [][]byte, err error) {
	if i == nil {
		delta = append(delta, rdx.Stlv(o.Name))
		delta = append(delta, rdx.Itlv(o.Age))
		return
	}
	had := uint64(0)
	for i.Next(); i.Valid(); i.Next() {
		id, rdt := chotki.OKeyIdRdt(i.Key())
		switch id.Off() {
		case 0:
			if rdt != 'O' {
				return nil, chotki.ErrBadORecord
			}
			z := rdx.IDFromZipBytes(i.Value())
			if z != SomeClassClassID {
				return nil, chotki.ErrBadORecord
			}
		case 1:
			if rdt != 'S' {
				break
			}
			d := rdx.Sdelta(i.Value(), o.Name)
			delta = append(delta, chotki.EditTLV(1, 'S', d))
			had = had | (1 << 1)
		case 2:
			if rdt != 'I' {
				break
			}
			d := rdx.Idelta(i.Value(), o.Age)
			delta = append(delta, chotki.EditTLV(2, 'I', d))
			had = had | (1 << 2)
		}
	}
	for off := uint64(1); off < 3; off++ {
		if (had & (1 << off)) == 0 {
			delta = append(delta, chotki.EditTLV(off, 'S', rdx.Stlv(o.Name)))
			delta = append(delta, chotki.EditTLV(off, 'I', rdx.Itlv(o.Age)))
		}
	}
	return delta, nil
}
