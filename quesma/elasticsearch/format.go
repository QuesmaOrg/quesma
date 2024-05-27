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
