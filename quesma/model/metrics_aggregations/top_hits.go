package metrics_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"strconv"
	"strings"
)

type TopHits struct {
	ctx                context.Context
	isSortFieldPresent bool
}

func NewTopHits(ctx context.Context, isSortFieldPresent bool) TopHits {
	return TopHits{ctx: ctx, isSortFieldPresent: isSortFieldPresent}
}

func (query TopHits) IsBucketAggregation() bool {
	return false
}

// TODO implement correct
func (query TopHits) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	var topElems []any
	if len(rows) > 0 && level >= len(rows[0].Cols)-1 {
		// values are [level, len(row.Cols) - 1]
		logger.WarnWithCtx(query.ctx).Msgf(
			"no columns returned for top_metrics aggregation, level: %d, len(rows[0].Cols): %d, len(rows): %d",
			level, len(rows[0].Cols), len(rows),
		)
	}
	for _, row := range rows {
		if len(row.Cols) == 0 {
			logger.WarnWithCtx(query.ctx).Msg("no columns returned for top_metrics aggregation, skipping")
			continue
		}

		var sortVal []any
		var valuesForMetrics []model.QueryResultCol
		if query.isSortFieldPresent {
			// per convention, we know that value we sorted by is in the last column (if it exists)
			lastIndex := len(row.Cols) - 1 // last column is the sort column, we don't return it
			sortVal = append(sortVal, row.Cols[lastIndex].Value)
			valuesForMetrics = row.Cols[level:lastIndex]
		} else {
			valuesForMetrics = row.Cols[level:]
		}
		hits := make(model.JsonMap)
		column := ""
		for _, col := range valuesForMetrics {
			var withoutQuotes string
			if unquoted, err := strconv.Unquote(col.ColName); err == nil {
				withoutQuotes = unquoted
			} else {
				withoutQuotes = col.ColName
			}
			colName, _ := strings.CutPrefix(withoutQuotes, `windowed_`)
			hits[colName] = col.ExtractValue(query.ctx)
			column = col.OverridenColName
		}
		elem := model.JsonMap{
			"_source": model.JsonMap{
				"sort": sortVal,
				column: hits,
			},
		}
		topElems = append(topElems, elem)
	}
	return []model.JsonMap{{
		"hits": topElems,
	}}
}

func (query TopHits) String() string {
	return "top_hits"
}

func (query TopHits) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
