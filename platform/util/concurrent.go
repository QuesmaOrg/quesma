// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package util

import "sync"

// SyncMap is a generics-friendly wrapper on top of sync.Map with a few extra handy methods,
// especially Snapshot since sync.Map can't be used with 'range' directly
type SyncMap[K comparable, V any] struct {
	syncSyncMap *sync.Map
}

type SyncMapEntry[K comparable, V any] struct {
	Key   K
	Value V
}

func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{syncSyncMap: &sync.Map{}}
}

func NewSyncMapWith[K comparable, V any](k K, v V) *SyncMap[K, V] {
	m := NewSyncMap[K, V]()
	m.Store(k, v)
	return m
}

func NewSyncMapFrom[K comparable, V any](initial map[K]V) *SyncMap[K, V] {
	m := NewSyncMap[K, V]()
	for k, v := range initial {
		m.Store(k, v)
	}
	return m
}

func NewSyncMapWithN[K comparable, V any](entries ...SyncMapEntry[K, V]) *SyncMap[K, V] {
	m := NewSyncMap[K, V]()
	for _, entry := range entries {
		m.Store(entry.Key, entry.Value)
	}
	return m
}

func (m *SyncMap[K, V]) Size() int {
	size := 0
	m.syncSyncMap.Range(func(_, _ any) bool {
		size++
		return true
	})
	return size
}

// Snapshot returns a copy of the map
func (m *SyncMap[K, V]) Snapshot() map[K]V {
	snapshot := make(map[K]V)
	m.syncSyncMap.Range(func(key, value any) bool {
		snapshot[key.(K)] = value.(V)
		return true
	})
	return snapshot
}

func (m *SyncMap[K, V]) Delete(key K) { m.syncSyncMap.Delete(key) }

func (m *SyncMap[K, V]) Load(key K) (value V, ok bool) {
	v, ok := m.syncSyncMap.Load(key)
	if !ok {
		return value, ok
	}
	return v.(V), ok
}
func (m *SyncMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, loaded := m.syncSyncMap.LoadAndDelete(key)
	if !loaded {
		return value, loaded
	}
	return v.(V), loaded
}
func (m *SyncMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	a, loaded := m.syncSyncMap.LoadOrStore(key, value)
	return a.(V), loaded
}
func (m *SyncMap[K, V]) Range(f func(key K, value V) bool) {
	m.syncSyncMap.Range(func(key, value any) bool { return f(key.(K), value.(V)) })
}
func (m *SyncMap[K, V]) Store(key K, value V) { m.syncSyncMap.Store(key, value) }

func (m *SyncMap[K, V]) Keys() []K {
	keys := make([]K, 0)
	m.syncSyncMap.Range(func(key, _ any) bool {
		keys = append(keys, key.(K))
		return true
	})
	return keys
}

func (m *SyncMap[K, V]) Has(key K) bool {
	_, ok := m.Load(key)
	return ok
}
