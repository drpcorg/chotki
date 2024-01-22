package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUint64Heap_Pop(t *testing.T) {
	h := Uint64Heap{}
	for i := uint64(0); i < 64; i++ {
		h.Push(i ^ 17)
	}
	for i := uint64(0); i < 64; i++ {
		assert.Equal(t, i, h.Pop())
	}
}
