package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTLVAppend(t *testing.T) {
	buf := []byte{}
	buf = Append(buf, 'A', []byte{'A'})
	buf = Append(buf, 'b', []byte{'B', 'B'})
	correct2 := []byte{'a', 1, 'A', '2', 'B', 'B'}
	assert.Equal(t, correct2, buf, "basic TLV fail")

	var c256 [256]byte
	for n := range c256 {
		c256[n] = 'c'
	}
	buf = Append(buf, 'C', c256[:])
	assert.Equal(t, len(correct2)+1+4+len(c256), len(buf))
	assert.Equal(t, uint8(67), buf[len(correct2)])
	assert.Equal(t, uint8(1), buf[len(correct2)+2])

	lit, body, buf, err := TakeAnyWary(buf)
	assert.Nil(t, err)
	assert.Equal(t, uint8('A'), lit)
	assert.Equal(t, []byte{'A'}, body)

	body2, _, err2 := TakeWary('B', buf)
	assert.Nil(t, err2)
	assert.Equal(t, []byte{'B', 'B'}, body2)
}

func TestFeedHeader(t *testing.T) {
	buf := []byte{}
	l, buf := OpenHeader(buf, 'A')
	text := "some text"
	buf = append(buf, text...)
	CloseHeader(buf, l)
	lit, body, rest, err := TakeAnyWary(buf)
	assert.Nil(t, err)
	assert.Equal(t, uint8('A'), lit)
	assert.Equal(t, text, string(body))
	assert.Equal(t, 0, len(rest))
}

func TestTinyRecord(t *testing.T) {
	body := "12"
	tiny := TinyRecord('X', []byte(body))
	assert.Equal(t, "212", string(tiny))
}
