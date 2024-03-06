package concurrent

import "sync"

// Map is a generics-friendly wrapper on top of sync.Map with a few extra handy methods,
// especially Snapshot since sync.Map can't be used with 'range' directly
type Map[K comparable, V any] struct {
	syncMap *sync.Map
}

type MapEntry[K comparable, V any] struct {
	Key   K
	Value V
}

func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{syncMap: &sync.Map{}}
}

func NewMapWith[K comparable, V any](k K, v V) *Map[K, V] {
	m := NewMap[K, V]()
	m.Store(k, v)
	return m
}

func NewMapWithN[K comparable, V any](entries ...MapEntry[K, V]) *Map[K, V] {
	m := NewMap[K, V]()
	for _, entry := range entries {
		m.Store(entry.Key, entry.Value)
	}
	return m
}

func (m *Map[K, V]) Size() int {
	size := 0
	m.syncMap.Range(func(_, _ any) bool {
		size++
		return true
	})
	return size
}

// Snapshot returns a copy of the map
func (m *Map[K, V]) Snapshot() map[K]V {
	snapshot := make(map[K]V)
	m.syncMap.Range(func(key, value any) bool {
		snapshot[key.(K)] = value.(V)
		return true
	})
	return snapshot
}

func (m *Map[K, V]) Delete(key K) { m.syncMap.Delete(key) }

func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	v, ok := m.syncMap.Load(key)
	if !ok {
		return value, ok
	}
	return v.(V), ok
}
func (m *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, loaded := m.syncMap.LoadAndDelete(key)
	if !loaded {
		return value, loaded
	}
	return v.(V), loaded
}
func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	a, loaded := m.syncMap.LoadOrStore(key, value)
	return a.(V), loaded
}
func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	m.syncMap.Range(func(key, value any) bool { return f(key.(K), value.(V)) })
}
func (m *Map[K, V]) Store(key K, value V) { m.syncMap.Store(key, value) }

func (m *Map[K, V]) Keys() []K {
	keys := make([]K, 0)
	m.syncMap.Range(func(key, _ any) bool {
		keys = append(keys, key.(K))
		return true
	})
	return keys
}
