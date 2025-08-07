package classes

// A class contains a number of fields. Each Field has
// some RDT type. A class can inherit another class.
// New fields can be appended to a class, but never removed.
// Max number of fields is 128, max inheritance depth 32.
// When stored, a class is an append-only sequence of Ts.
// The syntax for each T: "XName", where X is the RDT.
// For the map types, can use "MSS_Name" or similar.
// Each field has an Offset. The Offset+RdxType pair is the
// *actual key* for the field in the database.
// Entries having identical Offset+RdxType are considered *renames*!

import "unicode/utf8"

type IndexType byte

const (
	HashIndex     IndexType = 'H'
	FullscanIndex IndexType = 'F'
)

type Field struct {
	Offset     int64
	Name       string
	RdxType    byte
	RdxTypeExt []byte
	Index      IndexType
}

// Fields
type Fields []Field

func (f Field) Valid() bool {
	for _, l := range f.Name { // has unsafe chars
		if l < ' ' {
			return false
		}
	}

	return (f.RdxType >= 'A' && f.RdxType <= 'Z' &&
		len(f.Name) > 0 && utf8.ValidString(f.Name))
}

func (fs Fields) MaxOffset() (off int64) {
	for _, f := range fs {
		if f.Offset > off {
			off = f.Offset
		}
	}
	return
}

func (f Fields) FindRdtOff(rdx byte, off int64) int {
	for i := 0; i < len(f); i++ {
		if f[i].RdxType == rdx && f[i].Offset == off {
			return i
		}
	}
	return -1
}

func (f Fields) FindName(name string) (ndx int) { // fixme double naming?
	for i := 0; i < len(f); i++ {
		if f[i].Name == name {
			return i
		}
	}
	return -1
}
