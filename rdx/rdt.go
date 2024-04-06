package rdx

type RDT interface {
	Merge(tlvs [][]byte)
	State() (tlv []byte)

	String() string
	ToString(txt string, src uint64) error

	Native() interface{}
	ToNative(new_val interface{}, src uint64) (delta []byte)

	Diff(vvdiff VV) (diff []byte)
}
