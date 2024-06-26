// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

func Pointer[T any](d T) *T {
	return &d
}
