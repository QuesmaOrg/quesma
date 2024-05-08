package util

import "time"

// ParseTime parses time from string in RFC3339Nano format, and discards error. Returns just time.Time value.
func ParseTime(asString string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, asString)
	return t
}
