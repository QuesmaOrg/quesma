// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import "fmt"

func PrettyTestName(name string, idx int) string {
	return fmt.Sprintf("%s(%d)", name, idx)
}
