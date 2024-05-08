package bucket_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"strconv"
	"strings"
	"time"
)

type DateHistogram struct {
	ctx      context.Context
	Interval string
}

func NewDateHistogram(ctx context.Context, interval string) DateHistogram {
	return DateHistogram{ctx, interval}
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
		key := row.Cols[level-1].Value.(int64) * intervalInMilliseconds
		intervalStart := time.UnixMilli(key).UTC().Format("2006-01-02T15:04:05.000")
		response = append(response, model.JsonMap{
			"key":           key,
			"doc_count":     row.Cols[level].Value,
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
