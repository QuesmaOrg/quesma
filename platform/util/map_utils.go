// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"cmp"
	"fmt"
	"github.com/hashicorp/go-multierror"
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

// Merge function mutates the first map - use with caution!
func Merge[V any](m1, m2 map[string]V, errorContext string) (map[string]V, error) {
	var err *multierror.Error
	for k, v := range m2 {
		if _, exists := m1[k]; exists {
			err = multierror.Append(err, fmt.Errorf("key %s already exists. overriding (%s)", k, errorContext))
		}
		m1[k] = v
	}
	return m1, err.ErrorOrNil()
}
