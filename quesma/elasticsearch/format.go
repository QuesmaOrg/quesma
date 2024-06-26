// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import "time"

func FormatSortValue(i any) any {
	switch v := i.(type) {
	case time.Time:
		// When returned as part of `sort`, timestamps are always returned as millis
		return v.UnixMilli()
	default:
		return i
	}
}
