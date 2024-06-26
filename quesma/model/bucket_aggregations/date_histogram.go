// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bucket_aggregations

import (
	"context"
	"quesma/logger"
	"quesma/model"
	"strconv"
	"strings"
	"time"
)

const DefaultMinDocCount = 1

type DateHistogram struct {
	ctx         context.Context
	minDocCount int
	Interval    string
}

func NewDateHistogram(ctx context.Context, minDocCount int, interval string) DateHistogram {
	return DateHistogram{ctx, minDocCount, interval}
}

func (query DateHistogram) IsBucketAggregation() bool {
	return true
}

func (query DateHistogram) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	if len(rows) > 0 && len(rows[0].Cols) < 2 {
		logger.ErrorWithCtx(query.ctx).Msgf(
			"unexpected number of columns in date_histogram aggregation response, len(rows[0].Cols): "+
				"%d, level: %d", len(rows[0].Cols), level,
		)
	}
	var response []model.JsonMap
	for _, row := range rows {
		intervalInMilliseconds := query.IntervalAsDuration().Milliseconds()
		var key int64
		if keyValue, ok := row.Cols[len(row.Cols)-2].Value.(int64); ok { // used to be [level-1], but because some columns are duplicated, it doesn't work in 100% cases now
			key = keyValue * intervalInMilliseconds
		} else {
			logger.WarnWithCtx(query.ctx).Msgf("unexpected type of key value: %T, %+v, Should be int64", row.Cols[len(row.Cols)-2].Value, row.Cols[len(row.Cols)-2].Value)
		}
		intervalStart := time.UnixMilli(key).UTC().Format("2006-01-02T15:04:05.000")
		response = append(response, model.JsonMap{
			"key":           key,
			"doc_count":     row.LastColValue(), // used to be [level], but because some columns are duplicated, it doesn't work in 100% cases now
			"key_as_string": intervalStart,
		})
	}
	return response
}

func (query DateHistogram) String() string {
	return "date_histogram(interval: " + query.Interval + ")"
}

// TODO implement this also for intervals longer than days ("d")
func (query DateHistogram) IntervalAsDuration() time.Duration {
	// time.ParseDuration doesn't accept > hours
	if strings.HasSuffix(query.Interval, "d") {
		daysNr, err := strconv.Atoi(strings.TrimSuffix(query.Interval, "d"))
		if err != nil {
			logger.ErrorWithCtx(query.ctx).Msgf("error parsing interval %s: [%v]. Returning 0", query.Interval, err)
			return time.Duration(0)
		}
		intervalInHours := strconv.Itoa(daysNr*24) + "h"
		duration, _ := time.ParseDuration(intervalInHours)
		return duration
	}
	duration, _ := time.ParseDuration(query.Interval)
	return duration
}

// we're sure len(row.Cols) >= 2
func (query DateHistogram) getKey(row model.QueryResultRow) int64 {
	return row.Cols[len(row.Cols)-2].Value.(int64)
}

// if minDocCount == 0, and we have buckets e.g. [key, value1], [key+10, value2], we need to insert [key+1, 0], [key+2, 0]...
// CAUTION: a different kind of postprocessing is needed for minDocCount > 1, but I haven't seen any query with that yet, so not implementing it now.
func (query DateHistogram) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
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
