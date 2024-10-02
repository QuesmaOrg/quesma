// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"quesma/clickhouse"
	"quesma/kibana"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
	"strconv"
	"strings"
	"time"
)

type DateHistogramIntervalType bool

const (
	DefaultMinDocCount                                      = -1
	DateHistogramFixedInterval    DateHistogramIntervalType = true
	DateHistogramCalendarInterval DateHistogramIntervalType = false
	defaultDateTimeType                                     = clickhouse.DateTime64
	// OriginalKeyName is an original date_histogram's key, as it came from our SQL request.
	// It's needed when date_histogram has subaggregations, because when we process them, we're merging subaggregation's
	// map (it has the original key, doesn't know about the processed one)
	// with date_histogram's map (it already has a "valid", processed key, after TranslateSqlResponseToJson)
	OriginalKeyName = "__quesma_originalKey"
)

type DateHistogram struct {
	ctx               context.Context
	field             model.Expr // name of the field, e.g. timestamp
	interval          string
	timezone          string
	minDocCount       int
	intervalType      DateHistogramIntervalType
	fieldDateTimeType clickhouse.DateTimeType
}

func NewDateHistogram(ctx context.Context, field model.Expr, interval, timezone string,
	minDocCount int, intervalType DateHistogramIntervalType, fieldDateTimeType clickhouse.DateTimeType) *DateHistogram {
	return &DateHistogram{ctx: ctx, field: field, interval: interval, timezone: timezone,
		minDocCount: minDocCount, intervalType: intervalType, fieldDateTimeType: fieldDateTimeType}
}

func (typ DateHistogramIntervalType) String(ctx context.Context) string {
	switch typ {
	case DateHistogramFixedInterval:
		return "fixed_interval"
	case DateHistogramCalendarInterval:
		return "calendar_interval"
	default:
		logger.ErrorWithCtx(ctx).Msgf("unexpected DateHistogramIntervalType: %v", typ) // error as it should be impossible
		return "invalid"
	}
}

func (query *DateHistogram) AggregationType() model.AggregationType {
	return model.BucketAggregation
}

func (query *DateHistogram) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {

	if len(rows) > 0 && len(rows[0].Cols) < 2 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in date_histogram aggregation response, len(rows[0].Cols): %d",
			len(rows[0].Cols),
		)
	}

	// TODO:
	// Implement default when query.minDocCount == DefaultMinDocCount, we need to return
	// all buckets between the first bucket that matches documents and the last one.

	if query.minDocCount == 0 {
		rows = query.NewRowsTransformer().Transform(query.ctx, rows)
	}

	// key is in `query.timezone` time, and we need it to be UTC
	wantedTimezone, err := time.LoadLocation(query.timezone)
	if err != nil {
		logger.ErrorWithCtx(query.ctx).Msgf("time.LoadLocation error: %v", err)
		wantedTimezone = time.UTC
	}

	var response []model.JsonMap
	for _, row := range rows {
		var key int64
		docCount := row.LastColValue()
		if util.ExtractInt64(docCount) < int64(query.minDocCount) {
			continue
		}

		originalKey := query.getKey(row)
		if query.intervalType == DateHistogramCalendarInterval {
			key = originalKey
		} else {
			intervalInMilliseconds := query.intervalAsDuration().Milliseconds()
			key = originalKey * intervalInMilliseconds
		}

		ts := time.UnixMilli(key).UTC()
		intervalStartNotUTC := time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), ts.Nanosecond(), wantedTimezone)

		_, timezoneOffsetInSeconds := intervalStartNotUTC.Zone()
		key -= int64(timezoneOffsetInSeconds * 1000) // seconds -> milliseconds

		response = append(response, model.JsonMap{
			OriginalKeyName: originalKey,
			"key":           key,
			"doc_count":     docCount, // used to be [level], but because some columns are duplicated, it doesn't work in 100% cases now
			"key_as_string": time.UnixMilli(key).UTC().Format("2006-01-02T15:04:05.000"),
		})
	}

	return model.JsonMap{
		"buckets": response,
	}
}

func (query *DateHistogram) String() string {
	return fmt.Sprintf("date_histogram(field: %v, interval: %v, min_doc_count: %v, timezone: %v",
		query.field, query.interval, query.minDocCount, query.timezone)
}

// only intervals <= days are needed
func (query *DateHistogram) intervalAsDuration() time.Duration {
	var intervalInHoursOrLess string
	if strings.HasSuffix(query.interval, "d") {
		// time.ParseDuration doesn't accept > hours, we need to convert days to hours
		daysNr, err := strconv.Atoi(strings.TrimSuffix(query.interval, "d"))
		if err != nil {
			logger.ErrorWithCtx(query.ctx).Msgf("error parsing interval %s: [%v]. Returning 0", query.interval, err)
			return time.Duration(0)
		}
		intervalInHoursOrLess = strconv.Itoa(daysNr*24) + "h"
	} else {
		intervalInHoursOrLess = query.interval
	}
	duration, _ := time.ParseDuration(intervalInHoursOrLess)
	return duration
}

func (query *DateHistogram) GenerateSQL() model.Expr {
	switch query.intervalType {
	case DateHistogramFixedInterval:
		return query.generateSQLForFixedInterval()
	case DateHistogramCalendarInterval:
		return query.generateSQLForCalendarInterval()
	default:
		logger.WarnWithCtx(query.ctx).Msgf("invalid interval type: %v (should be impossible). Returning InvalidExpr",
			query.intervalType.String(query.ctx))
		return model.InvalidExpr
	}
}

func (query *DateHistogram) generateSQLForFixedInterval() model.Expr {
	interval, err := kibana.ParseInterval(query.interval)
	if err != nil {
		logger.ErrorWithCtx(query.ctx).Msg(err.Error())
	}
	dateTimeType := query.fieldDateTimeType
	if query.fieldDateTimeType == clickhouse.Invalid {
		logger.ErrorWithCtx(query.ctx).Msgf("invalid date type for DateHistogram %+v. Using DateTime64 as default.", query)
		dateTimeType = defaultDateTimeType
	}
	return clickhouse.TimestampGroupByWithTimezone(query.field, dateTimeType, interval, query.timezone)
}

func (query *DateHistogram) generateSQLForCalendarInterval() model.Expr {
	const defaultTimezone = "UTC"
	exprForBiggerIntervals := func(toIntervalStartFuncName string) model.Expr {
		// returned expr as string:
		// 1000 * toInt64(toUnixTimestamp(toStartOf[Week|Month|Quarter|Year](toTimeZone(timestamp, timezone)))

		timezone := query.timezone
		if timezone == "" {
			timezone = defaultTimezone
		}
		timestampFieldWithOffset := model.NewFunction("toTimezone", query.field, model.NewLiteral(fmt.Sprintf("'%s'", timezone)))

		toStartOf := model.NewFunction(toIntervalStartFuncName, timestampFieldWithOffset) // toStartOfMonth(...) or toStartOfWeek(...)
		toUnixTimestamp := model.NewFunction("toUnixTimestamp", toStartOf)                // toUnixTimestamp(toStartOf...)
		toInt64 := model.NewFunction("toInt64", toUnixTimestamp)                          // toInt64(toUnixTimestamp(...))
		return model.NewInfixExpr(toInt64, "*", model.NewLiteral(1000))                   // toInt64(...)*1000
	}

	// calendar_interval: minute/hour/day are the same as fixed_interval: 1m/1h/1d
	switch query.interval {
	case "minute", "1m":
		query.interval = "1m"
		query.intervalType = DateHistogramFixedInterval
		return query.generateSQLForFixedInterval()
	case "hour", "1h":
		query.interval = "1h"
		query.intervalType = DateHistogramFixedInterval
		return query.generateSQLForFixedInterval()
	case "day", "1d":
		query.interval = "1d"
		query.intervalType = DateHistogramFixedInterval
		return query.generateSQLForFixedInterval()
	case "week", "1w":
		return exprForBiggerIntervals("toStartOfWeek")
	case "month", "1M":
		return exprForBiggerIntervals("toStartOfMonth")
	case "quarter", "1q":
		return exprForBiggerIntervals("toStartOfQuarter")
	case "year", "1y":
		return exprForBiggerIntervals("toStartOfYear")
	}

	logger.WarnWithCtx(query.ctx).Msgf("unexpected calendar interval: %s. Returning InvalidExpr", query.interval)
	return model.InvalidExpr
}

func (query *DateHistogram) getKey(row model.QueryResultRow) int64 {
	return row.Cols[len(row.Cols)-2].Value.(int64)
}

func (query *DateHistogram) NewRowsTransformer() model.QueryRowsTransformer {
	differenceBetweenTwoNextKeys := int64(1)
	if query.intervalType == DateHistogramCalendarInterval {
		duration, err := kibana.ParseInterval(query.interval)
		if err != nil {
			logger.ErrorWithCtx(query.ctx).Err(err)
		}
		differenceBetweenTwoNextKeys = duration.Milliseconds()
	}
	return &DateHistogramRowsTransformer{minDocCount: query.minDocCount, differenceBetweenTwoNextKeys: differenceBetweenTwoNextKeys}
}

// we're sure len(row.Cols) >= 2

type DateHistogramRowsTransformer struct {
	minDocCount                  int
	differenceBetweenTwoNextKeys int64
}

// if minDocCount == 0, and we have buckets e.g. [key, value1], [key+10, value2], we need to insert [key+1, 0], [key+2, 0]...
// CAUTION: a different kind of postprocessing is needed for minDocCount > 1, but I haven't seen any query with that yet, so not implementing it now.
func (qt *DateHistogramRowsTransformer) Transform(ctx context.Context, rowsFromDB []model.QueryResultRow) []model.QueryResultRow {

	if qt.minDocCount != 0 || len(rowsFromDB) < 2 {
		// we only add empty rows, when
		// a) minDocCount == 0
		// b) we have > 1 rows, with < 2 rows we can't add anything in between
		return rowsFromDB
	}
	if qt.minDocCount < 0 {
		logger.WarnWithCtx(ctx).Msgf("unexpected negative minDocCount: %d. Skipping postprocess", qt.minDocCount)
		return rowsFromDB
	}
	postprocessedRows := make([]model.QueryResultRow, 0, len(rowsFromDB))
	postprocessedRows = append(postprocessedRows, rowsFromDB[0])
	for i := 1; i < len(rowsFromDB); i++ {
		fmt.Println(i, len(rowsFromDB[i-1].Cols), len(rowsFromDB[i].Cols))
		if len(rowsFromDB[i-1].Cols) < 2 || len(rowsFromDB[i].Cols) < 2 {
			logger.ErrorWithCtx(ctx).Msgf(
				"unexpected number of columns in date_histogram aggregation response (< 2),"+
					"rowsFromDB[%d]: %+v, rowsFromDB[%d]: %+v. Skipping those rows in postprocessing",
				i-1, rowsFromDB[i-1], i, rowsFromDB[i],
			)
		}
		lastKey := qt.getKey(rowsFromDB[i-1])
		currentKey := qt.getKey(rowsFromDB[i])
		for midKey := lastKey + qt.differenceBetweenTwoNextKeys; midKey < currentKey; midKey += qt.differenceBetweenTwoNextKeys {
			fmt.Println(midKey-lastKey, midKey)
			midRow := rowsFromDB[i-1].Copy()
			midRow.Cols[len(midRow.Cols)-2].Value = midKey
			midRow.Cols[len(midRow.Cols)-1].Value = 0
			postprocessedRows = append(postprocessedRows, midRow)
		}
		postprocessedRows = append(postprocessedRows, rowsFromDB[i])
	}
	return postprocessedRows
}

func (qt *DateHistogramRowsTransformer) getKey(row model.QueryResultRow) int64 {
	return row.Cols[len(row.Cols)-2].Value.(int64)
}
