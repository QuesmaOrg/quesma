// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"cmp"
	"sort"
)

func MapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func MapValues[K comparable, V any](m map[K]V) []V {
	keys := make([]V, 0, len(m))
	for _, v := range m {
		keys = append(keys, v)
	}
	return keys
}

func MapKeysSorted[K cmp.Ordered, V any](m map[K]V) []K {
	keys := MapKeys(m)
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func MapKeysSortedByValue[K comparable, V cmp.Ordered](m map[K]V) []K {
	keys := MapKeys(m)
	sort.Slice(keys, func(i, j int) bool {
		return m[keys[i]] < m[keys[j]]
	})
	return keys
}
