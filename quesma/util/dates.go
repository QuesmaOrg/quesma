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

// DaysInMonth returns number of days in month of given time.
func DaysInMonth(t time.Time) int {
	// a bit of a heuristic, but it should work + I evade some edge cases by doing so
	return int(t.AddDate(0, 1, 0).Sub(t).Hours()+0.1) / 24
}

// FindTimestampPrecision returns the number of digits after the dot in the seconds part of the timestamp.
// e.g. '2024-12-21 07:29:03.123456789' -> 9, as it has 9 digits after the dot.
// It only works for timestamps looking like this: '2024-12-21 07:29:03[.digit+]'
// For timestamps without dot, it returns 0 (e.g. '2024-12-21 07:29:03').
func FindTimestampPrecision(timestamp string) (precision int, success bool) {
	isTime := func(endIdx int) bool {
		// timestamp[:endIdx] should end with 'DD:DD:DD' (D is a digit)
		shouldBeDigitIdxs := []int{endIdx - 1, endIdx - 2, endIdx - 4, endIdx - 5, endIdx - 7, endIdx - 8}
		shouldBeColonIdxs := []int{endIdx - 3, endIdx - 6}
		if endIdx-8 < 0 {
			return false
		}
		for _, idx := range shouldBeDigitIdxs {
			if timestamp[idx] < '0' || timestamp[idx] > '9' {
				return false
			}
		}
		for _, idx := range shouldBeColonIdxs {
			if timestamp[idx] != ':' {
				return false
			}
		}
		return true
	}

	lastDot := strings.LastIndex(timestamp, ".")
	if lastDot == -1 {
		return 0, isTime(len(timestamp))
	} else {
		if !isTime(lastDot) {
			return -1, false
		} else {
			for i := lastDot + 1; i < len(timestamp); i++ {
				if timestamp[i] < '0' || timestamp[i] > '9' {
					return -1, false
				}
				precision += 1
			}
		}
		return precision, true
	}
}
