package rdx

import (
	"github.com/drpcorg/chotki/protocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAmerge(t *testing.T) {
	tlv := Atlv(LogT{
		"Ione",
		"Stwo",
		"Rthree",
	})
	tlv2 := Atlv(LogT{
		"Ione",
		"Stwo",
		"Rthree",
		"Tfour",
	})
	tlv2b := Amerge(protocol.Records{tlv, tlv2})
	assert.Equal(t, tlv2, tlv2b)

	one := protocol.Record('T', FIRSTtlv(3, 0, []byte("Tfour")))
	tlv3 := Amerge(protocol.Records{tlv, one})
	assert.Equal(t, tlv2, tlv3)

	tlv4 := Amerge(protocol.Records{tlv, one, tlv2, tlv3})
	assert.Equal(t, tlv2, tlv4)
}
