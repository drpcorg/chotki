package utils

import "golang.org/x/exp/constraints"

type Heap[T constraints.Ordered] struct {
	buf []T
}

func (h *Heap[T]) Len() int {
	return len(h.buf)
}

// Push pushes the element x onto the heap.
// The complexity is O(log n) where n = h.Len().
func (h *Heap[T]) Push(x T) {
	h.buf = append(h.buf, x)
	h.up(h.Len() - 1)
}

func (h *Heap[T]) swap(i, j int) {
	h.buf[i], h.buf[j] = h.buf[j], h.buf[i]
}

// Pop removes and returns the minimum element (according to Less) from the heap.
// The complexity is O(log n) where n = h.Len().
// Pop is equivalent to Remove(h, 0).
func (h *Heap[T]) Pop() (min T) {
	min = h.buf[0]
	n := h.Len() - 1
	h.swap(0, n)
	h.down(0, n)
	h.buf = h.buf[0:n]
	return
}

// Remove removes and returns the element at index i from the heap.
// The complexity is O(log n) where n = h.Len().
func (h *Heap[T]) Remove(i int) T {
	n := h.Len() - 1
	if n != i {
		h.swap(i, n)
		if !h.down(i, n) {
			h.up(i)
		}
	}
	return h.Pop()
}

// Fix re-establishes the heap ordering after the element at index i has changed its value.
// Changing the value of the element at index i and then calling Fix is equivalent to,
// but less expensive than, calling Remove(h, i) followed by a Push of the new value.
// The complexity is O(log n) where n = h.Len().
func (h Heap[T]) Fix(i int) {
	if !h.down(i, h.Len()) {
		h.up(i)
	}
}

func (h Heap[T]) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !(h.buf[j] < h.buf[i]) {
			break
		}
		h.buf[i], h.buf[j] = h.buf[j], h.buf[i]
		j = i
	}
}

func (h Heap[T]) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.buf[j2] < h.buf[j1] {
			j = j2 // = 2*i + 2  // right child
		}
		if !(h.buf[j] < h.buf[i]) {
			break
		}
		h.buf[i], h.buf[j] = h.buf[j], h.buf[i]
		i = j
	}
	return i > i0
}
