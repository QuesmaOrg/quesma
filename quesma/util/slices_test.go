package util

import (
	"cmp"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDistinct(t *testing.T) {
	type testCase[T cmp.Ordered] struct {
		name  string
		elems []T
		want  []T
	}
	tests := []testCase[string]{
		{
			name:  "should return nil for nil slice",
			elems: nil,
			want:  nil,
		},
		{
			name:  "should return empty for empty slice",
			elems: []string{},
			want:  []string{},
		},
		{
			name:  "should return same slice for distinct elements",
			elems: []string{"a", "b", "c"},
			want:  []string{"a", "b", "c"},
		},
		{
			name:  "should return distinct elements",
			elems: []string{"a", "b", "a", "c", "b"},
			want:  []string{"a", "b", "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Distinct(tt.elems), "Distinct(%v)", tt.elems)
		})
	}
}
