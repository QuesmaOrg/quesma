package bucket_aggregations

import (
	"mitmproxy/quesma/model"
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
	// fmt.Println(level) comment helps me in next PRs
	for _, row := range rows {
		intervalInMilliseconds := qt.IntervalAsDuration().Milliseconds()
		key := row.Cols[level].Value.(int64) * intervalInMilliseconds
		intervalStart := time.UnixMilli(key).UTC().Format("2006-01-02T15:04:05.000")
		response = append(response, model.JsonMap{
			"key":           key,
			"doc_count":     row.Cols[level+1].Value,
			"key_as_string": intervalStart,
		})
	}
	return response
}

func (qt QueryTypeDateHistogram) String() string {
	return "date_histogram"
}

func (qt QueryTypeDateHistogram) IntervalAsDuration() time.Duration {
	duration, _ := time.ParseDuration(qt.Interval)
	return duration
}
