package util

import "testing"

func TestIsSmaller(t *testing.T) {
	var testcases = []struct {
		a, b   float64
		wanted bool
	}{
		{1.0, 2.0, true},
		{2.0, 1.0, false},
		{1.0, 1.0, false},
		{1.0, 1.0 + 1e-10, false},
		{0.99999999999, 1.0, false},
		{0.9999, 1.0, true},
	}
	for _, tc := range testcases {
		if got := IsSmaller(tc.a, tc.b); got != tc.wanted {
			t.Errorf("IsSmaller(%f, %f) = %v, want %v", tc.a, tc.b, got, tc.wanted)
		}
	}
}
