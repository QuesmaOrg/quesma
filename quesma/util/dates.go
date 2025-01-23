// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"strings"
	"time"
)

// ParseTime parses time from string in RFC3339Nano format, and discards error. Returns just time.Time value.
func ParseTime(asString string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, asString)
	return t
}

// FindTimestampPrecision returns the number of digits after the dot in the seconds part of the timestamp.
// It only works for timestamps looking like this: '2024-12-21 07:29:03.3[more_digits+]' (here return value is 1).
// For timestamps without dot, it returns 0 (e.g. '2024-12-21 07:29:03').
func FindTimestampPrecision(timestamp string) (precision int, success bool) {
	isTime := func(endIdx int) bool {
		// timestamp[:endIdx] should end with 'DD:DD:DD' (D is a digit)
		shouldBeDigit := []int{endIdx - 1, endIdx - 2, endIdx - 4, endIdx - 5, endIdx - 7, endIdx - 8}
		shouldBeColon := []int{endIdx - 3, endIdx - 6}
		if endIdx-8 < 0 {
			return false
		}
		for _, idx := range shouldBeDigit {
			if timestamp[idx] < '0' || timestamp[idx] > '9' {
				return false
			}
		}
		for _, idx := range shouldBeColon {
			if timestamp[idx] != ':' {
				return false
			}
		}
		return true
	}

	lastDot := strings.LastIndex(timestamp, ".")
	if lastDot == -1 {
		success = isTime(len(timestamp))
	} else {
		if !isTime(lastDot) {
			success = false
		} else {
			success = true
			for i := lastDot + 1; i < len(timestamp); i++ {
				if timestamp[i] < '0' || timestamp[i] > '9' {
					success = false
				}
				precision += 1
			}
		}
	}
	return
}
