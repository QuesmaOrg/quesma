// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"fmt"
	"time"
)

// ParseTime parses time from string in RFC3339Nano format, and discards error. Returns just time.Time value.
func ParseTime(asString string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, asString)
	return t
}

func DaysInMonth(t time.Time) int {
	fmt.Println("KK DaysInMonth, ", t, int(t.AddDate(0, 1, 0).Sub(t).Hours()+0.1)/24)
	return int(t.AddDate(0, 1, 0).Sub(t).Hours()+0.1) / 24
}
