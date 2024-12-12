// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

// IsSmaller checks if a is smaller than b (with a small epsilon, due to float inaccuracies)
func IsSmaller(a, b float64) bool {
	return (a + 1e-9) < b
}

// IsInt32 checks if 'shouldBeFloat64' is a float64, that is actually an integer small enough to be an int
func IsInt32(shouldBeFloat64 any) bool {
	f, ok := shouldBeFloat64.(float64)
	return ok && f == float64(int(f))
}
