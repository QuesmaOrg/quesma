package kibana

import (
	"strconv"
	"strings"
	"time"
)

func ParseInterval(fixedInterval string) (time.Duration, error) {
	var unit time.Duration

	switch {
	case strings.HasSuffix(fixedInterval, "d"):
		unit = 24 * time.Hour
	case strings.HasSuffix(fixedInterval, "w"):
		unit = 7 * 24 * time.Hour
	case strings.HasSuffix(fixedInterval, "M"):
		unit = 30 * 24 * time.Hour
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
