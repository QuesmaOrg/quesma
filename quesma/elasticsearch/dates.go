// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"quesma/util"
	"strconv"
	"time"
)

type DateManager struct{}

// when missing is a string and its value as int < 10000, so e.g. "9999", it means 01.01.9999
// >= 10000 means unix timestamp with that value
const yearOrTsDelimiter = 10000

var formats = []string{"2006", "2006-01", "2006-01-02", "2006-01-02"}

func (dm DateManager) MissingInDateHistogramToUnixTimestamp(missing any) (unixTimestamp int64, parsingSucceded bool) {
	asInt, success := util.ExtractInt64Maybe(missing)
	if success {
		return asInt, true
	}
	asFloat, success := util.ExtractFloat64Maybe(missing)
	if success {
		return int64(asFloat), true
	}

	asString, success := missing.(string)
	if !success {
		return -1, false
	}

	// if the string is a number >= 10000, it's already a timestamp
	var err error
	if asInt, err = strconv.ParseInt(asString, 10, 64); err == nil && asInt >= yearOrTsDelimiter {
		return dm.MissingInDateHistogramToUnixTimestamp(asInt)
	}
	if asFloat, err = strconv.ParseFloat(asString, 64); err == nil && asFloat >= yearOrTsDelimiter {
		return dm.MissingInDateHistogramToUnixTimestamp(asFloat)
	}

	var date time.Time
	for _, format := range formats {
		if date, err = time.Parse(format, asString); err == nil {
			return date.UnixMilli(), true
		}
	}

	return -1, false
}
