// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"testing"
)

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

func TestIsFloat64AnInt64(t *testing.T) {
	var testcases = []struct {
		f      float64
		wanted bool
	}{
		{1.0, true},
		{1.1, false},
		{1.0000000000000001, true},
		{1.0000000000000002, false},
		{1000000000000.0, true},
		{1000000000000001.0, true},
		{1000000000000001.1, false},
	}
	for _, tc := range testcases {
		if got := IsFloat64AnInt64(tc.f); got != tc.wanted {
			t.Errorf("IsFloat64AnInt64(%f) = %v, want %v", tc.f, got, tc.wanted)
		}
	}
}
