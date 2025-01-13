// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"strconv"
	"strings"
	"time"
)

func ParseInterval(fixedInterval string) (time.Duration, error) {
	var unit time.Duration

	switch fixedInterval {
	case "minute":
		return time.Minute, nil
	case "hour":
		return time.Hour, nil
	case "day":
		return time.Hour * 24, nil
	case "week":
		return time.Hour * 24 * 7, nil
	case "month":
		return time.Hour * 24 * 30, nil
	case "quarter":
		return time.Hour * 24 * 30 * 3, nil
	case "year":
		return time.Hour * 24 * 365, nil
	}

	switch {
	case strings.HasSuffix(fixedInterval, "d"):
		unit = 24 * time.Hour
	case strings.HasSuffix(fixedInterval, "w"):
		unit = 7 * 24 * time.Hour
	case strings.HasSuffix(fixedInterval, "M"):
		unit = 30 * 24 * time.Hour
	case strings.HasSuffix(fixedInterval, "q"):
		unit = 3 * 30 * 24 * time.Hour
	case strings.HasSuffix(fixedInterval, "y"):
		unit = 365 * 24 * time.Hour
	default:
		return time.ParseDuration(fixedInterval)
	}

	value, err := strconv.Atoi(fixedInterval[:len(fixedInterval)-1])
	if err != nil {
		return 0, err
	}

	return time.Duration(value) * unit, nil
}
