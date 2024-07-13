// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"quesma/clickhouse"
	"quesma/kibana"
	"quesma/logger"
	"quesma/model"
	"strconv"
	"strings"
	"time"
)

type DateHistogramIntervalType bool

const (
	DefaultMinDocCount                                      = 1
	DateHistogramFixedInterval    DateHistogramIntervalType = true
	DateHistogramCalendarInterval DateHistogramIntervalType = false
	defaultDateTimeType                                     = clickhouse.DateTime64
)

type DateHistogram struct {
	ctx               context.Context
	field             model.Expr // name of the field, e.g. timestamp
	interval          string
	minDocCount       int
	intervalType      DateHistogramIntervalType
	fieldDateTimeType clickhouse.DateTimeType
}

func NewDateHistogram(ctx context.Context, field model.Expr, interval string,
	minDocCount int, intervalType DateHistogramIntervalType, fieldDateTimeType clickhouse.DateTimeType) *DateHistogram {
	return &DateHistogram{ctx: ctx, field: field, interval: interval,
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

func (query *DateHistogram) IsBucketAggregation() bool {
	return true
}

func (query *DateHistogram) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	if len(rows) > 0 && len(rows[0].Cols) < 2 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in date_histogram aggregation response, len(rows[0].Cols): "+
				"%d, level: %d", len(rows[0].Cols), level,
		)
	}
	var response []model.JsonMap
	for _, row := range rows {
		var key int64
		if query.intervalType == DateHistogramCalendarInterval {
			key = query.getKey(row)
		} else {
			intervalInMilliseconds := query.intervalAsDuration().Milliseconds()
			key = query.getKey(row) * intervalInMilliseconds
		}

		intervalStart := time.UnixMilli(key).UTC().Format("2006-01-02T15:04:05.000")
		response = append(response, model.JsonMap{
			"key":           key,
			"doc_count":     row.LastColValue(), // used to be [level], but because some columns are duplicated, it doesn't work in 100% cases now
			"key_as_string": intervalStart,
		})
	}
	return model.JsonMap{
		"buckets": response,
	}
}

func (query *DateHistogram) String() string {
	return "date_histogram(interval: " + query.interval + ")"
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
	return clickhouse.TimestampGroupBy(query.field, dateTimeType, interval)
}

func (query *DateHistogram) generateSQLForCalendarInterval() model.Expr {
	exprForBiggerIntervals := func(toIntervalStartFuncName string) model.Expr {
		// returned expr as string:
		// "1000 * toInt64(toUnixTimestamp(toStartOf[Week|Month|Quarter|Year](timestamp)))"
		toStartOf := model.NewFunction(toIntervalStartFuncName, query.field)
		toUnixTimestamp := model.NewFunction("toUnixTimestamp", toStartOf)
		toInt64 := model.NewFunction("toInt64", toUnixTimestamp)
		return model.NewInfixExpr(toInt64, "*", model.NewLiteral(1000))
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

// we're sure len(row.Cols) >= 2
func (query *DateHistogram) getKey(row model.QueryResultRow) int64 {
	return row.Cols[len(row.Cols)-2].Value.(int64)
}

// if minDocCount == 0, and we have buckets e.g. [key, value1], [key+10, value2], we need to insert [key+1, 0], [key+2, 0]...
// CAUTION: a different kind of postprocessing is needed for minDocCount > 1, but I haven't seen any query with that yet, so not implementing it now.
func (query *DateHistogram) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	if query.minDocCount != 0 || len(rowsFromDB) < 2 {
		// we only add empty rows, when
		// a) minDocCount == 0
		// b) we have > 1 rows, with < 2 rows we can't add anything in between
		return rowsFromDB
	}
	if query.minDocCount < 0 {
		logger.WarnWithCtx(query.ctx).Msgf("unexpected negative minDocCount: %d. Skipping postprocess", query.minDocCount)
		return rowsFromDB
	}
	postprocessedRows := make([]model.QueryResultRow, 0, len(rowsFromDB))
	postprocessedRows = append(postprocessedRows, rowsFromDB[0])
	for i := 1; i < len(rowsFromDB); i++ {
		if len(rowsFromDB[i-1].Cols) < 2 || len(rowsFromDB[i].Cols) < 2 {
			logger.ErrorWithCtx(query.ctx).Msgf(
				"unexpected number of columns in date_histogram aggregation response (< 2),"+
					"rowsFromDB[%d]: %+v, rowsFromDB[%d]: %+v. Skipping those rows in postprocessing",
				i-1, rowsFromDB[i-1], i, rowsFromDB[i],
			)
		}
		lastKey := query.getKey(rowsFromDB[i-1])
		currentKey := query.getKey(rowsFromDB[i])
		for midKey := lastKey + 1; midKey < currentKey; midKey++ {
			midRow := rowsFromDB[i-1].Copy()
			midRow.Cols[len(midRow.Cols)-2].Value = midKey
			midRow.Cols[len(midRow.Cols)-1].Value = 0
			postprocessedRows = append(postprocessedRows, midRow)
		}
		postprocessedRows = append(postprocessedRows, rowsFromDB[i])
	}
	return postprocessedRows
}
