package rdx

import (
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/learn-decentralized-systems/toytlv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEmerge(t *testing.T) {
	tlv1 := Eparse("{1, 2, \"four\"}")
	assert.Equal(t, "{1,2,\"four\"}", Estring(tlv1))
	tlv2 := Eparse("{3, \"four\", 5}")
	tlv12 := Emerge(toyqueue.Records{tlv1, tlv2})
	str12 := Estring(tlv12)
	assert.Equal(t, "{1,2,3,\"four\",5}", str12)

	del := Itlve(-1, 0, 1)
	tlv12d := Emerge(toyqueue.Records{tlv1, tlv2, del})
	str12d := Estring(tlv12d)
	assert.Equal(t, "{2,3,\"four\",5}", str12d)
}

func TestMmerge(t *testing.T) {
	tlv1 := Mparse("{1: 2, 3: 4, 5:6}")
	assert.Equal(t, "{1:2,3:4,5:6}", Mstring(tlv1))
	tlv2 := Mparse("{3:4, 5:6, 7:8}")
	tlv12 := Mmerge(toyqueue.Records{tlv1, tlv2})
	str12 := Mstring(tlv12)
	assert.Equal(t, "{1:2,3:4,5:6,7:8}", str12)

	del := toytlv.Concat(
		Itlve(-1, 0, 5),
		Itlve(-1, 0, 6), // todo T
	)
	tlv12d := Mmerge(toyqueue.Records{tlv1, tlv2, del})
	str12d := Mstring(tlv12d)
	assert.Equal(t, "{1:2,3:4,7:8}", str12d)
}
