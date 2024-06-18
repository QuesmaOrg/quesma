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
