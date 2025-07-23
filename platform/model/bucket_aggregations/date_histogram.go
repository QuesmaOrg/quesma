// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/util"
	"strconv"
	"strings"
	"time"
)

type DateHistogramIntervalType bool

const (
	DateHistogramFixedInterval    DateHistogramIntervalType = true
	DateHistogramCalendarInterval DateHistogramIntervalType = false
	defaultDateTimeType                                     = database_common.DateTime64
	// OriginalKeyName is an original date_histogram's key, as it came from our SQL request.
	// It's needed when date_histogram has subaggregations, because when we process them, we're merging subaggregation's
	// map (it has the original key, doesn't know about the processed one)
	// with date_histogram's map (it already has a "valid", processed key, after TranslateSqlResponseToJson)
	OriginalKeyName      = "__quesma_originalKey"
	NoExtendedBound      = int64(-1) // -1 and not e.g. 0, as 0 is a valid value
	maxEmptyBucketsAdded = 100000
)

type DateHistogram struct {
	ctx               context.Context
	field             model.Expr // name of the field, e.g. timestamp
	interval          string
	timezone          string
	defaultFormat     bool           // how we format "key_as_string". If not default, it's milliseconds
	wantedTimezone    *time.Location // key is in `timezone` time, and we need it to be UTC
	extendedBoundsMin int64
	extendedBoundsMax int64
	minDocCount       int
	intervalType      DateHistogramIntervalType
	fieldDateTimeType database_common.DateTimeType
}

func NewDateHistogram(ctx context.Context, field model.Expr, interval, timezone, format string, minDocCount int,
	extendedBoundsMin, extendedBoundsMax int64, intervalType DateHistogramIntervalType, fieldDateTimeType database_common.DateTimeType) *DateHistogram {

	wantedTimezone, err := time.LoadLocation(timezone)
	if err != nil {
		logger.ErrorWithCtx(ctx).Msgf("time.LoadLocation error: %v", err)
		wantedTimezone = time.UTC
	}

	defaultFormat := format != "epoch_millis"

	return &DateHistogram{ctx: ctx, field: field, interval: interval, timezone: timezone, wantedTimezone: wantedTimezone,
		minDocCount: minDocCount, extendedBoundsMin: extendedBoundsMin, extendedBoundsMax: extendedBoundsMax,
		intervalType: intervalType, fieldDateTimeType: fieldDateTimeType, defaultFormat: defaultFormat}
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

	if query.minDocCount == 0 || query.extendedBoundsMin != NoExtendedBound || query.extendedBoundsMax != NoExtendedBound {
		rows = query.NewRowsTransformer().Transform(query.ctx, rows)
	}

	response := make([]model.JsonMap, 0, len(rows))
	for _, row := range rows {
		docCount := row.LastColValue()
		docCountAsInt, err := util.ExtractInt64(docCount)
		if err != nil {
			logger.ErrorWithCtx(query.ctx).Msgf("error parsing doc_count: %v", docCount)
		}
		if docCountAsInt < int64(query.minDocCount) {
			continue
		}
		originalKey := query.getKey(row)
		responseKey := query.calculateResponseKey(originalKey)
		var keyAsString string
		if query.defaultFormat {
			keyAsString = query.calculateKeyAsString(responseKey)
		} else {
			keyAsString = strconv.FormatInt(responseKey, 10)
		}
		response = append(response, model.JsonMap{
			OriginalKeyName: originalKey,
			"key":           responseKey,
			"doc_count":     docCount,
			"key_as_string": keyAsString,
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
	interval, err := util.ParseInterval(query.interval)
	if err != nil {
		logger.ErrorWithCtx(query.ctx).Msg(err.Error())
	}
	dateTimeType := query.fieldDateTimeType
	if query.fieldDateTimeType == database_common.Invalid {
		logger.ErrorWithCtx(query.ctx).Msgf("invalid date type for DateHistogram %+v. Using DateTime64 as default.", query)
		dateTimeType = defaultDateTimeType
	}
	return database_common.TimestampGroupByWithTimezone(query.field, dateTimeType, interval, query.timezone)
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
		query.interval = "1w"
		return exprForBiggerIntervals("toStartOfWeek")
	case "month", "1M":
		query.interval = "1M"
		return exprForBiggerIntervals("toStartOfMonth")
	case "quarter", "1q":
		query.interval = "1q"
		return exprForBiggerIntervals("toStartOfQuarter")
	case "year", "1y":
		query.interval = "1y"
		return exprForBiggerIntervals("toStartOfYear")
	}

	logger.WarnWithCtx(query.ctx).Msgf("unexpected calendar interval: %s. Returning InvalidExpr", query.interval)
	return model.InvalidExpr
}

func (query *DateHistogram) getKey(row model.QueryResultRow) int64 {
	return extractRowValue(row)
}

func (query *DateHistogram) Interval() (interval time.Duration, ok bool) {
	if duration, err := util.ParseInterval(query.interval); err == nil {
		return duration, true
	} else {
		logger.WarnWithCtx(query.ctx).Msg(err.Error())
		return 0, false
	}
}

func (query *DateHistogram) calculateResponseKeyInUTC(originalKey int64) int64 {
	if query.intervalType == DateHistogramCalendarInterval {
		return originalKey
	}
	intervalInMilliseconds := query.intervalAsDuration().Milliseconds()
	return originalKey * intervalInMilliseconds
}

// originalKey is the key as it came from our SQL request (e.g. returned by query.getKey)
func (query *DateHistogram) calculateResponseKey(originalKey int64) int64 {
	keyInUTC := query.calculateResponseKeyInUTC(originalKey)

	ts := time.UnixMilli(keyInUTC)
	intervalStartNotUTC := time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), ts.Nanosecond(), query.wantedTimezone)

	_, timezoneOffsetInSeconds := intervalStartNotUTC.Zone()
	return keyInUTC - int64(timezoneOffsetInSeconds*1000) // seconds -> milliseconds
}

func (query *DateHistogram) fromUTCToWantedTimezone(tsUTC int64) int64 {
	dateUTC := time.UnixMilli(tsUTC)
	date := time.Date(dateUTC.Year(), dateUTC.Month(), dateUTC.Day(), dateUTC.Hour(), dateUTC.Minute(), dateUTC.Second(), dateUTC.Nanosecond(), query.wantedTimezone)

	_, timezoneOffsetInSeconds := date.Zone()
	return tsUTC + int64(timezoneOffsetInSeconds*1000) // seconds -> milliseconds
}

func (query *DateHistogram) calculateKeyAsString(key int64) string {
	return time.UnixMilli(key).UTC().Format("2006-01-02T15:04:05.000") // TODO: check if this necessary Format("2006/01/02 15:04:05")
}

func (query *DateHistogram) OriginalKeyToKeyAsString(originalKey any) string {
	responseKey := query.calculateResponseKey(originalKey.(int64))
	return query.calculateKeyAsString(responseKey)
}

func (query *DateHistogram) SetMinDocCountToZero() {
	query.minDocCount = 0
}

func (query *DateHistogram) NewRowsTransformer() model.QueryRowsTransformer {
	duration, err := util.ParseInterval(query.interval)
	var differenceBetweenTwoNextKeys int64
	if err == nil {
		differenceBetweenTwoNextKeys = duration.Milliseconds()
	} else {
		// 0 is fine value for differenceBetweenTwoNextKeys, as it means we don't add keys
		logger.ErrorWithCtx(query.ctx).Err(err)
	}
	return &DateHistogramRowsTransformer{dateHistogram: query, MinDocCount: query.minDocCount,
		differenceBetweenTwoNextKeys: differenceBetweenTwoNextKeys, EmptyValue: 0,
		extendedBoundsMin: query.extendedBoundsMin, extendedBoundsMax: query.extendedBoundsMax}
}

// we're sure len(row.Cols) >= 2

type DateHistogramRowsTransformer struct {
	dateHistogram                *DateHistogram
	differenceBetweenTwoNextKeys int64 // if 0, we don't add keys
	extendedBoundsMin            int64 // simply copied from DateHistogram
	extendedBoundsMax            int64 // simply copied from DateHistogram
	MinDocCount                  int
	EmptyValue                   any
}

// if MinDocCount == 0, and we have buckets e.g. [key, value1], [key+10, value2], we need to insert [key+1, 0], [key+2, 0]...
// Also if extendedBounds are present, we need to add all keys between them.
// CAUTION: a different kind of postprocessing is needed for MinDocCount > 1, but I haven't seen any query with that yet, so not implementing it now.
func (qt *DateHistogramRowsTransformer) Transform(ctx context.Context, rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	if qt.MinDocCount != 0 || qt.differenceBetweenTwoNextKeys == 0 {
		// we only add empty rows, when
		// a) MinDocCount == 0
		// b) we have valid differenceBetweenTwoNextKeys (>0)
		return rowsFromDB
	}
	if qt.MinDocCount < 0 {
		logger.WarnWithCtx(ctx).Msgf("unexpected negative MinDocCount: %d. Skipping postprocess", qt.MinDocCount)
		return rowsFromDB
	}

	emptyRowsAdded := 0
	postprocessedRows := make([]model.QueryResultRow, 0, len(rowsFromDB))
	if len(rowsFromDB) > 0 {
		postprocessedRows = append(postprocessedRows, rowsFromDB[0])
	}

	// add "mid" keys, so any needed key between [first_row_key, last_row_key]
	for i := 1; i < len(rowsFromDB); i++ {
		if len(rowsFromDB[i-1].Cols) < 2 || len(rowsFromDB[i].Cols) < 2 {
			logger.ErrorWithCtx(ctx).Msgf(
				"unexpected number of columns in date_histogram aggregation response (< 2),"+
					"rowsFromDB[%d]: %+v, rowsFromDB[%d]: %+v. Skipping those rows in postprocessing",
				i-1, rowsFromDB[i-1], i, rowsFromDB[i],
			)
		}
		lastKey := qt.dateHistogram.calculateResponseKeyInUTC(qt.getKey(rowsFromDB[i-1]))
		currentKey := qt.dateHistogram.calculateResponseKeyInUTC(qt.getKey(rowsFromDB[i]))

		// ugly, but works, will do for now
		doWeDivide := (currentKey/qt.getKey(rowsFromDB[i])) >= 100 || (float64(currentKey)/float64(qt.getKey(rowsFromDB[i]))) <= 0.01

		for midKey := qt.nextKey(lastKey); midKey < currentKey && emptyRowsAdded < maxEmptyBucketsAdded; midKey = qt.nextKey(midKey) {
			midRow := rowsFromDB[i-1].Copy()
			divideBy := int64(1)
			if doWeDivide {
				divideBy = qt.differenceBetweenTwoNextKeys
			}
			midRow.Cols[len(midRow.Cols)-2].Value = midKey / divideBy
			midRow.Cols[len(midRow.Cols)-1].Value = qt.EmptyValue

			postprocessedRows = append(postprocessedRows, midRow)
			emptyRowsAdded++
		}
		postprocessedRows = append(postprocessedRows, rowsFromDB[i])
	}

	// some cases where we don't need to add anything more
	switch {
	case qt.extendedBoundsMax == NoExtendedBound && qt.extendedBoundsMin == NoExtendedBound:
	case len(postprocessedRows) == 0 && (qt.extendedBoundsMax == NoExtendedBound || qt.extendedBoundsMin == NoExtendedBound):
		return postprocessedRows

	}
	noBounds := qt.extendedBoundsMax == NoExtendedBound && qt.extendedBoundsMin == NoExtendedBound
	noRowsAndNotFullyBounded := len(postprocessedRows) == 0 && (qt.extendedBoundsMax == NoExtendedBound || qt.extendedBoundsMin == NoExtendedBound)
	if noBounds || noRowsAndNotFullyBounded {
		return postprocessedRows
	}

	newRow := func(key int64) model.QueryResultRow {
		var row model.QueryResultRow
		if len(postprocessedRows) > 0 {
			row = postprocessedRows[0].Copy()
			row.Cols[len(row.Cols)-2].Value = key
			row.Cols[len(row.Cols)-1].Value = qt.EmptyValue
		} else {
			row = model.QueryResultRow{
				Cols: []model.QueryResultCol{
					model.NewQueryResultCol("", key),
					model.NewQueryResultCol("", qt.EmptyValue),
				},
			}
		}
		return row
	}

	// add "pre" keys, so any needed key between [extendedBoundsMin, first_row_key]
	if qt.extendedBoundsMin != NoExtendedBound {
		firstRequiredKey := (qt.dateHistogram.fromUTCToWantedTimezone(qt.extendedBoundsMin) + qt.differenceBetweenTwoNextKeys - 1) / qt.differenceBetweenTwoNextKeys
		var lastRequiredKey int64
		if len(postprocessedRows) > 0 {
			lastRequiredKey = qt.getKey(postprocessedRows[0])
			if qt.dateHistogram.intervalType == DateHistogramCalendarInterval {
				lastRequiredKey /= qt.differenceBetweenTwoNextKeys
			}
		} else {
			// we know qt.extendedBoundsMax != NoExtendedBound, because we would've returned earlier - line below is safe
			lastRequiredKey = qt.dateHistogram.fromUTCToWantedTimezone(qt.extendedBoundsMax) / qt.differenceBetweenTwoNextKeys
		}
		preRows := make([]model.QueryResultRow, 0, max(0, int(lastRequiredKey-firstRequiredKey)))
		for preKey := firstRequiredKey; preKey < lastRequiredKey && emptyRowsAdded < maxEmptyBucketsAdded; preKey++ {
			preRows = append(preRows, newRow(preKey))
			emptyRowsAdded++
		}

		postprocessedRows = append(preRows, postprocessedRows...)
	}

	// add "post" keys, so any needed key between [last_row_key, extendedBoundsMax]
	if qt.extendedBoundsMax != NoExtendedBound {
		firstRequiredKey := qt.dateHistogram.calculateResponseKeyInUTC(qt.getKey(postprocessedRows[len(postprocessedRows)-1]))/qt.differenceBetweenTwoNextKeys + 1
		lastRequiredKey := qt.dateHistogram.fromUTCToWantedTimezone(qt.extendedBoundsMax) / qt.differenceBetweenTwoNextKeys
		for postKey := firstRequiredKey; postKey <= lastRequiredKey && emptyRowsAdded < maxEmptyBucketsAdded; postKey++ {
			postprocessedRows = append(postprocessedRows, newRow(postKey))
			emptyRowsAdded++
		}
	}

	return postprocessedRows
}

func (qt *DateHistogramRowsTransformer) getKey(row model.QueryResultRow) int64 {
	return extractRowValue(row)
}

func extractRowValue(row model.QueryResultRow) int64 {
	value := row.Cols[len(row.Cols)-2].Value
	switch v := value.(type) {
	case int64:
		return v
	case string:
		val, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			logger.Error().Msgf("string conver to int64 failed %T", v)
			return 0
		}
		return val
	default:
		logger.Error().Msgf("unsupported key type: %T", v)
		return 0
	}
}

func (qt *DateHistogramRowsTransformer) nextKey(key int64) int64 {
	if qt.dateHistogram.intervalType == DateHistogramFixedInterval {
		return key + qt.differenceBetweenTwoNextKeys
	}
	if qt.dateHistogram.interval != "1M" && qt.dateHistogram.interval != "1q" && qt.dateHistogram.interval != "1y" {
		// intervals < month are the same as fixed_interval here
		return key + qt.differenceBetweenTwoNextKeys
	}

	addNMonths := func(key int64, N int) int64 {
		ts := time.UnixMilli(key).UTC()
		// adding 2 days below isn't exactly necessary, it's only a quick way to make sure we're in the same month, even for weird timezones
		return ts.AddDate(0, N, 2).UnixMilli() - ts.AddDate(0, 0, 2).UnixMilli()
	}
	var monthsNr int
	switch qt.dateHistogram.interval {
	case "1M":
		monthsNr = 1
	case "1q":
		monthsNr = 3
	case "1y":
		monthsNr = 12
	}
	deltaInMs := addNMonths(key, monthsNr)
	return key + deltaInMs
}
