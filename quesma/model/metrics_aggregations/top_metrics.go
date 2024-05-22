package metrics_aggregations

import (
	"context"
	"github.com/k0kubun/pp"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"strconv"
	"strings"
)

type TopMetrics struct {
	ctx context.Context
}

func NewTopMetrics(ctx context.Context) TopMetrics {
	return TopMetrics{ctx: ctx}
}

func (query TopMetrics) IsBucketAggregation() bool {
	return false
}

func (query TopMetrics) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var topElems []any
	if len(rows) > 0 && level >= len(rows[0].Cols)-1 {
		// values are [level, len(row.Cols) - 1]
		logger.WarnWithCtx(query.ctx).Msgf(
			"no columns returned for top_metrics aggregation, level: %d, len(rows[0].Cols): %d, len(rows): %d",
			level, len(rows[0].Cols), len(rows),
		)
	}
	pp.Println(rows[0], level)
	for _, row := range rows {
		lastIndex := len(row.Cols) - 1 // per convention, we know that value we sorted by is in the last column
		metrics := make(model.JsonMap)
		valuesForMetrics := row.Cols[:lastIndex]
		sortVal := row.Cols[lastIndex].Value
		for _, col := range valuesForMetrics[level:] {
			var withoutQuotes string
			if unquoted, err := strconv.Unquote(col.ColName); err == nil {
				withoutQuotes = unquoted
			} else {
				withoutQuotes = col.ColName
			}
			colName, _ := strings.CutPrefix(withoutQuotes, `windowed_`)
			metrics[colName] = col.ExtractValue(query.ctx) // CHANGE IT AFTER PART 2 MERGE!! ENTER REAL CONTEXT FROM THE query
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

func (query TopMetrics) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
