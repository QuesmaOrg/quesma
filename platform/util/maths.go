// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"math"
	"strconv"
)

// IsSmaller checks if a is smaller than b (with a small epsilon, due to float inaccuracies)
func IsSmaller(a, b float64) bool {
	return (a + 1e-9) < b
}

// IsFloat64AnInt64 checks if a float64 holds an integer value (small enough to be represented as an int64, and not lose precision while casting)
// Careful about precision loss: ~2^53 is the maximum integer that can be represented as a float64 without losing precision
func IsFloat64AnInt64(f float64) bool {
	return f == math.Ceil(f) && f <= float64(math.MaxInt64)
}

// IsFloat checks if a string is a float
func IsFloat(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

// ToFloat converts a string to float64
func ToFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// IsInt checks if a string is an integer (in range of int64)
func IsInt(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

// ToInt64 converts a string to int64
func ToInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
