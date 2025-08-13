// Defines Host interfaces for Chotki
package host

import (
	"encoding/binary"

	"github.com/drpcorg/chotki/rdx"
)

const SyncBlockBits = 28
const SyncBlockMask = uint64((1 << SyncBlockBits) - 1)

func OKey(id rdx.ID, rdt byte) (key []byte) {
	var ret = [18]byte{'O'}
	key = binary.BigEndian.AppendUint64(ret[:1], id.Src())
	key = binary.BigEndian.AppendUint64(key, id.Pro())
	key = append(key, rdt)
	return
}

const LidLKeyLen = 1 + 16 + 1

func OKeyIdRdt(key []byte) (id rdx.ID, rdt byte) {
	if len(key) != LidLKeyLen {
		return rdx.BadId, 0
	}

	id = rdx.IDFromBytes(key[1 : LidLKeyLen-1])
	rdt = key[LidLKeyLen-1]
	return
}

var VKey0 = []byte{'V', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'V'}

func VKey(id rdx.ID) (key []byte) {
	var ret = [18]byte{'V'}
	block := id.ProOr(SyncBlockMask)
	key = binary.BigEndian.AppendUint64(ret[:1], block.Src())
	key = binary.BigEndian.AppendUint64(key, block.Pro())
	key = append(key, 'V')
	return
}

func VKeyId(key []byte) rdx.ID {
	if len(key) != LidLKeyLen {
		return rdx.BadId
	}
	return rdx.IDFromBytes(key[1:]).ProAnd(^SyncBlockMask)
}

func ObjectKeyRange(oid rdx.ID) (fro, til []byte) {
	oid = oid.ZeroOff()
	return OKey(oid, 'O'), OKey(oid.IncPro(1), 0)
}
