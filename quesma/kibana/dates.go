// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package kibana

import (
	"context"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
	"strconv"
	"time"
)

type DateManager struct {
	ctx context.Context
}

func NewDateManager(ctx context.Context) DateManager {
	return DateManager{ctx}
}

var acceptableDateTimeFormats = []string{"2006", "2006-01", "2006-01-02", "2006-01-02", "2006-01-02T15",
	"2006-01-02T15:04", "2006-01-02T15:04:05", "2006-01-02T15:04:05Z07", "2006-01-02T15:04:05Z07:00"}

// parseStrictDateOptionalTimeOrEpochMillis parses date, which is in [strict_date_optional_time || epoch_millis] format
// (https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html)
func (dm DateManager) parseStrictDateOptionalTimeOrEpochMillis(date any) (unixTimestamp int64, parsingSucceeded bool) {
	if asInt, success := util.ExtractInt64Maybe(date); success {
		return asInt, true
	}

	if asFloat, success := util.ExtractFloat64Maybe(date); success {
		return int64(asFloat), true
	}

	asString, success := date.(string)
	if !success {
		return -1, false
	}

	// * When missing is a single number >= 10000, it's already a unix timestamp (e.g. "10000" -> 10000th second after 01.01.1970)
	//   And we'll fall into one of the ifs below.
	// * When missing is a single number < 10000, it's a year, so "2345" -> 01.01.2345 00:00
	//   It'll be caught be one of the formats from the loop below.
	const yearOrTsDelimiter = 10000

	if asInt, err := strconv.ParseInt(asString, 10, 64); err == nil && asInt >= yearOrTsDelimiter {
		return dm.parseStrictDateOptionalTimeOrEpochMillis(asInt)
	} else if asFloat, err := strconv.ParseFloat(asString, 64); err == nil && asFloat >= yearOrTsDelimiter {
		return dm.parseStrictDateOptionalTimeOrEpochMillis(asFloat)
	}

	// It could be replaced with iso8601.ParseString() after the fixes to 1.4.0:
	// https://github.com/relvacode/iso8601/pull/26
	for _, format := range acceptableDateTimeFormats {
		if date, err := time.Parse(format, asString); err == nil {
			return date.UnixMilli(), true
		}
	}

	return -1, false
}

// ParseDateUsualFormat parses date expression, which is in [strict_date_optional_time || epoch_millis] format
// (https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html)
// It's most usual format for date in Kibana, used e.g. in Query DSL's range, or date_histogram.
func (dm DateManager) ParseDateUsualFormat(exprFromRequest any, datetimeType clickhouse.DateTimeType) (
	resultExpr model.Expr, parsingSucceeded bool) {
	if unixTs, success := dm.parseStrictDateOptionalTimeOrEpochMillis(exprFromRequest); success {
		switch datetimeType {
		case clickhouse.DateTime64:
			return model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(unixTs)), true
		case clickhouse.DateTime:
			return model.NewFunction("fromUnixTimestamp", model.NewLiteral(unixTs/1000)), true
		default:
			logger.WarnWithCtx(dm.ctx).Msgf("Unknown datetimeType: %v", datetimeType)
		}
	}
	return nil, false
}
