package main

type Uint64Heap []uint64

func (uh Uint64Heap) Len() int {
	return len([]uint64(uh))
}

// Push pushes the element x onto the heap.
// The complexity is O(log n) where n = h.Len().
func (uh *Uint64Heap) Push(x uint64) {
	(*uh) = append(*uh, x)
	uh.up(uh.Len() - 1)
}

func (uh Uint64Heap) swap(i, j int) {
	uh[i], uh[j] = uh[j], uh[i]
}

// Pop removes and returns the minimum element (according to Less) from the heap.
// The complexity is O(log n) where n = h.Len().
// Pop is equivalent to Remove(h, 0).
func (uh *Uint64Heap) Pop() (min uint64) {
	min = (*uh)[0]
	n := uh.Len() - 1
	uh.swap(0, n)
	uh.down(0, n)
	(*uh) = (*uh)[0:n]
	return
}

// Remove removes and returns the element at index i from the heap.
// The complexity is O(log n) where n = h.Len().
func (uh *Uint64Heap) Remove(i int) uint64 {
	n := uh.Len() - 1
	if n != i {
		uh.swap(i, n)
		if !uh.down(i, n) {
			uh.up(i)
		}
	}
	return uh.Pop()
}

// Fix re-establishes the heap ordering after the element at index i has changed its value.
// Changing the value of the element at index i and then calling Fix is equivalent to,
// but less expensive than, calling Remove(h, i) followed by a Push of the new value.
// The complexity is O(log n) where n = h.Len().
func (uh Uint64Heap) Fix(i int) {
	if !uh.down(i, uh.Len()) {
		uh.up(i)
	}
}

func (uh Uint64Heap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !(uh[j] < uh[i]) {
			break
		}
		uh[i], uh[j] = uh[j], uh[i]
		j = i
	}
}

func (uh Uint64Heap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && uh[j2] < uh[j1] {
			j = j2 // = 2*i + 2  // right child
		}
		if !(uh[j] < uh[i]) {
			break
		}
		uh[i], uh[j] = uh[j], uh[i]
		i = j
	}
	return i > i0
}
