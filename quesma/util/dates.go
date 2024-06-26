// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import "time"

// ParseTime parses time from string in RFC3339Nano format, and discards error. Returns just time.Time value.
func ParseTime(asString string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, asString)
	return t
}
