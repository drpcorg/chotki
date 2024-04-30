package rdx

const (
	MergeA = iota
	MergeAB
	MergeB
	MergeBA
)

type SortedIterator interface {
	Next() bool
	Merge(b SortedIterator) int
	Value() []byte
}

type ItHeap[T SortedIterator] []T

func (ih *ItHeap[T]) Len() int {
	return len([]T(*ih))
}

func (ih *ItHeap[T]) Push(x T) {
	if !x.Next() {
		return
	}
	ih.push(x)
}

func (ih *ItHeap[T]) push(x T) {
	(*ih) = append(*ih, x)
	ih.up(ih.Len() - 1)
}

func (ih *ItHeap[T]) Next() (next []byte) {
	x := ih.Pop()
	next = x.Value()
	for ih.Len() > 0 && x.Merge((*ih)[0]) == MergeA {
		y := ih.Pop()
		if y.Next() {
			ih.push(y)
		}
	}
	if x.Next() {
		ih.push(x)
	}
	return
}

func (ih ItHeap[T]) swap(i, j int) {
	ih[i], ih[j] = ih[j], ih[i]
}

// Pop removes and returns the minimum element (according to Less) from the heap.
// The complexity is O(log n) where n = h.Len().
// Pop is equivalent to Remove(h, 0).
func (ih *ItHeap[T]) Pop() (min T) {
	min = (*ih)[0]
	n := ih.Len() - 1
	ih.swap(0, n)
	ih.down(0, n)
	(*ih) = (*ih)[0:n]
	return
}

// Remove removes and returns the element at index i from the heap.
// The complexity is O(log n) where n = h.Len().
func (ih *ItHeap[T]) Remove(i int) T {
	n := ih.Len() - 1
	if n != i {
		ih.swap(i, n)
		if !ih.down(i, n) {
			ih.up(i)
		}
	}
	return ih.Pop()
}

// Fix re-establishes the heap ordering after the element at index i has changed its value.
// Changing the value of the element at index i and then calling Fix is equivalent to,
// but less expensive than, calling Remove(h, i) followed by a push of the new value.
// The complexity is O(log n) where n = h.Len().
func (ih ItHeap[T]) Fix(i int) {
	if !ih.down(i, ih.Len()) {
		ih.up(i)
	}
}

func (ih ItHeap[T]) up(a int) {
	for {
		b := (a - 1) / 2 // parent
		cmp := ih[a].Merge(ih[b])
		if b == a || !(cmp <= MergeAB) {
			break
		}
		ih[b], ih[a] = ih[a], ih[b]
		a = b
	}
}

func (ih ItHeap[T]) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && ih[j2].Merge(ih[j1]) <= MergeAB {
			j = j2 // = 2*i + 2  // right child
		}
		if !(ih[j].Merge(ih[i]) <= MergeAB) {
			break
		}
		ih[i], ih[j] = ih[j], ih[i]
		i = j
	}
	return i > i0
}
