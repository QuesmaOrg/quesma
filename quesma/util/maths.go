// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

// IsSmaller checks if a is smaller than b (with a small epsilon, due to float inaccuracies)
func IsSmaller(a, b float64) bool {
	return (a + 1e-9) < b
}

// IsFloat64AnInt64 checks if a float64 holds an integer value (small enough to be represented as an int64, and not lose precision while casting)
// Careful about precision loss: ~2^53 is the maximum integer that can be represented as a float64 without losing precision
func IsFloat64AnInt64(f float64) bool {
	return f == float64(int64(f))
}
