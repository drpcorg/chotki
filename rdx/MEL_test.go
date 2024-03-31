package rdx

import (
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEmerge(t *testing.T) {
	tlv1 := Eparse("{1, 2, 4}")
	assert.Equal(t, "{1,2,4}", Estring(tlv1))
	tlv2 := Eparse("{3, 4, 5}")
	tlv12 := Emerge(toyqueue.Records{tlv1, tlv2})
	str12 := Estring(tlv12)
	assert.Equal(t, "{1,2,3,4,5}", str12)

	del := Itlve(-1, 0, 1)
	tlv12d := Emerge(toyqueue.Records{tlv1, tlv2, del})
	str12d := Estring(tlv12d)
	assert.Equal(t, "{2,3,4,5}", str12d)
}
