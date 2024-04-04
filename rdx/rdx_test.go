package rdx

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRDX_Parse(t *testing.T) {
	// todo
	// - separators
	// - more checks
	// - FIRST, object
	cases := map[string]string{
		"12345":                           "12345",
		"{1: 2}":                          "{1:2}",
		"{1: {2 : 4}}":                    "{1:{2:4}}",
		"[ 1, 2, 3]":                      "[1,2,3]",
		" [ \"long string here\", 1 ,2 ]": "[\"long string here\",1,2]",
		"{1f8-a364: 3 }":                  "{1f8-a364:3}",
		"{1f8-a364, 3,4, \"five\" }":      "{1f8-a364,3,4,\"five\"}",
	}
	for in, out := range cases {
		rdx, err := ParseRDX([]byte(in))
		assert.Nil(t, err)
		assert.Equal(t, out, rdx.String())
	}
}

func TestRDX_String(t *testing.T) {
	cases := map[string]string{
		"\"Alice has a big teddy bear\"": "Alice has a big teddy bear",
		"\"\\n\\t\\r\"":                  "\n\t\r",
		"\"Кириллически\"":               "Кириллически",
		"\"\"":                           "",
	}
	for in, out := range cases {
		rdx, err := ParseRDX([]byte(in))
		assert.Nil(t, err)
		assert.NotNil(t, rdx)
		assert.Equal(t, RdxString, rdx.RdxType)
		tlv := Sparse(string(rdx.Text))
		unesc := Snative(tlv)
		assert.Equal(t, out, unesc)
		str := Sstring(tlv)
		assert.Equal(t, in, str)
	}
}
