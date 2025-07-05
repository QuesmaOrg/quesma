// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package elastic_query_dsl

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/util"
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
	"2006-01-02T15:04", "2006-01-02T15:04:05", "2006-01-02T15:04:05Z07", "2006-01-02T15:04:05Z07:00",
	"2006-01-02 15:04:05 -0700 MST", "2006-01-02 15:04:05.000 -0700 MST", "2006-01-02 15:04:05.000000000 -0700 MST"}

// parseStrictDateOptionalTimeOrEpochMillis parses date, which is in [strict_date_optional_time || epoch_millis] format
// (https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html)
func (dm DateManager) parseStrictDateOptionalTimeOrEpochMillis(date any) (utcTimestamp time.Time, parsingSucceeded bool) {
	if asInt, success := util.ExtractInt64Maybe(date); success {
		return time.UnixMilli(asInt), true
	}

	if asFloat, success := util.ExtractFloat64Maybe(date); success {
		return time.UnixMilli(int64(asFloat)), true
	}

	asString, success := date.(string)
	if !success {
		return time.Time{}, false
	}

	// * When missing is a single number >= 10000, it's already a unix timestamp (e.g. "10000" -> 10000th second after 01.01.1970)
	//   And we'll fall into one of the ifs below.
	// * When missing is a single number < 10000, it's a year, so "2345" -> 01.01.2345 00:00
	//   It'll be caught be one of the formats from the loop below.
	const yearOrTsDelimiter = 10000

	if asInt, err := util.ToInt64(asString); err == nil && asInt >= yearOrTsDelimiter {
		return dm.parseStrictDateOptionalTimeOrEpochMillis(asInt)
	} else if asFloat, err := strconv.ParseFloat(asString, 64); err == nil && asFloat >= yearOrTsDelimiter {
		return dm.parseStrictDateOptionalTimeOrEpochMillis(asFloat)
	}

	// It could be replaced with iso8601.ParseString() after the fixes to 1.4.0:
	// https://github.com/relvacode/iso8601/pull/26
	for _, format := range acceptableDateTimeFormats {
		if t, err := time.Parse(format, asString); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

// ParseDateUsualFormat parses date expression, which is in [strict_date_optional_time || epoch_millis] format
// (https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html)
// It's most usual format for date in Kibana, used e.g. in Query DSL's range, or date_histogram.
func (dm DateManager) ParseDateUsualFormat(exprFromRequest any, datetimeType database_common.DateTimeType, format any) (
	resultExpr model.Expr, parsingSucceeded bool) {
	addFormat := func(v any) model.LiteralExpr {
		l := model.NewLiteral(v)
		if format != nil {
			l.Attrs[model.FormatKey] = format
		}
		return l
	}
	if utcTs, success := dm.parseStrictDateOptionalTimeOrEpochMillis(exprFromRequest); success {
		switch datetimeType {
		case database_common.DateTime64:
			threeDigitsOfPrecisionSuffice := utcTs.UnixNano()%1_000_000 == 0
			if threeDigitsOfPrecisionSuffice {
				return model.NewFunction("fromUnixTimestamp64Milli", addFormat(utcTs.UnixMilli())), true
			} else {
				return model.NewFunction(
					"toDateTime64",
					model.NewInfixExpr(
						addFormat(utcTs.UnixNano()),
						"/",
						model.NewLiteral(1_000_000_000),
					),
					model.NewLiteral(9),
				), true
			}
		case database_common.DateTime:
			return model.NewFunction("fromUnixTimestamp", addFormat(utcTs.Unix())), true
		default:
			logger.WarnWithCtx(dm.ctx).Msgf("Unknown datetimeType: %v", datetimeType)
		}
	}
	return nil, false
}
