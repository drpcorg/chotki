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
		"12345":        "12345",
		"{1: 2}":       "{1:2}",
		"{1: {2 : 4}}": "{1:{2:4}}",
	}
	for in, out := range cases {
		rdx, err := ParseRDX([]byte(in))
		assert.Nil(t, err)
		assert.Equal(t, out, rdx.String())
	}
}
