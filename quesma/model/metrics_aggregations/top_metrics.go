package metrics_aggregations

import (
	"mitmproxy/quesma/model"
	"strings"
)

type TopMetrics struct{}

func (query TopMetrics) IsBucketAggregation() bool {
	return false
}

func (query TopMetrics) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var topElems []any
	for _, row := range rows {
		lastIndex := len(row.Cols) - 1 // per convention, we know that value we sorted by is in the last column
		metrics := make(model.JsonMap)
		valuesForMetrics := row.Cols[:lastIndex]
		sortVal := row.Cols[lastIndex].Value
		for _, col := range valuesForMetrics[level:] {
			colName, _ := strings.CutPrefix(col.ColName, "windowed_")
			metrics[colName] = col.ExtractValue()
		}
		elem := model.JsonMap{
			"sort":    []interface{}{sortVal},
			"metrics": metrics,
		}
		topElems = append(topElems, elem)
	}
	return []model.JsonMap{{
		"top": topElems,
	}}
}

func (query TopMetrics) String() string {
	return "top_metrics"
}
