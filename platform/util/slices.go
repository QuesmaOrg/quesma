// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"cmp"
	"golang.org/x/exp/maps"
	"slices"
)

func Distinct[T cmp.Ordered](elems []T) []T {
	if elems == nil {
		return nil
	}
	var set = map[T]bool{}
	for _, elem := range elems {
		set[elem] = true
	}
	result := maps.Keys(set)
	slices.Sort(result)
	return result
}

// AppendFromIdx1 returns "append(to, from[1:]...) if len(from) >= 1 else to"
func AppendFromIdx1[T any](to, from []T) []T {
	if len(from) >= 1 {
		return append(to, from[1:]...)
	}
	return to
}

// AppendFromIdx2 returns "append(to, from[2:]...) if len(from) >= 2 else to"
func AppendFromIdx2[T any](to, from []T) []T {
	if len(from) >= 2 {
		return append(to, from[2:]...)
	}
	return to
}
