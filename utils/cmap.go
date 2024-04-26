package utils

import "sync"

type CMap[K comparable, V any] struct {
	sm sync.Map
}

func (m *CMap[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	return m.sm.CompareAndDelete(key, old)
}

func (m *CMap[K, V]) CompareAndSwap(key K, old, new V) bool {
	return m.sm.CompareAndSwap(key, old, new)
}

func (m *CMap[K, V]) Delete(key K) {
	m.sm.Delete(key)
}

func (m *CMap[K, V]) Load(key K) (value V, ok bool) {
	v, o := m.sm.Load(key)
	if !o {
		return value, o
	}
	return v.(V), o
}

func (m *CMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, l := m.sm.LoadAndDelete(key)
	if !l {
		return value, l
	}
	return v.(V), l
}

func (m *CMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	a, l := m.sm.LoadOrStore(key, value)
	return a.(V), l
}

func (m *CMap[K, V]) Range(f func(key K, value V) bool) {
	m.sm.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}

func (m *CMap[K, V]) Store(key K, value V) {
	m.sm.Store(key, value)
}

func (m *CMap[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	p, l := m.sm.Swap(key, value)
	return p.(V), l
}
