package bucket_aggregations

import (
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"strconv"
	"strings"
	"time"
)

type DateHistogram struct {
	Interval string
}

func (query DateHistogram) IsBucketAggregation() bool {
	return true
}

func (query DateHistogram) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
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
			logger.Error().Msgf("Error parsing interval %s: [%v]", query.Interval, err)
			return time.Duration(0)
		}
		intervalInHours := strconv.Itoa(daysNr*24) + "h"
		duration, _ := time.ParseDuration(intervalInHours)
		return duration
	}
	duration, _ := time.ParseDuration(query.Interval)
	return duration
}
