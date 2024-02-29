package bucket_aggregations

import (
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"strconv"
	"strings"
	"time"
)

type QueryTypeDateHistogram struct {
	Interval string
}

func (qt QueryTypeDateHistogram) IsBucketAggregation() bool {
	return true
}

func (qt QueryTypeDateHistogram) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var response []model.JsonMap
	// fmt.Println("date_histogram level: ", level) // very needed for debugging in next PRs
	for _, row := range rows {
		intervalInMilliseconds := qt.IntervalAsDuration().Milliseconds()
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

func (qt QueryTypeDateHistogram) String() string {
	return "date_histogram(interval: " + qt.Interval + ")"
}

// TODO implement this also for intervals longer than days ("d")
func (qt QueryTypeDateHistogram) IntervalAsDuration() time.Duration {
	// time.ParseDuration doesn't accept > hours
	if strings.HasSuffix(qt.Interval, "d") {
		daysNr, err := strconv.Atoi(strings.TrimSuffix(qt.Interval, "d"))
		if err != nil {
			logger.Error().Msgf("Error parsing interval %s: [%v]", qt.Interval, err)
			return time.Duration(0)
		}
		intervalInHours := strconv.Itoa(daysNr*24) + "h"
		duration, _ := time.ParseDuration(intervalInHours)
		return duration
	}
	duration, _ := time.ParseDuration(qt.Interval)
	return duration
}
