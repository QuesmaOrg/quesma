// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

// IsSmaller checks if a is smaller than b (with a small epsilon, due to float inaccuracies)
func IsSmaller(a, b float64) bool {
	return (a + 1e-9) < b
}
