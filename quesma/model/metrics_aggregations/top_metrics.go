// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"quesma/logger"
	"quesma/model"
	"strconv"
	"strings"
)

type TopMetrics struct {
	ctx                context.Context
	isSortFieldPresent bool
}

func NewTopMetrics(ctx context.Context, isSortFieldPresent bool) TopMetrics {
	return TopMetrics{ctx: ctx, isSortFieldPresent: isSortFieldPresent}
}

func (query TopMetrics) IsBucketAggregation() bool {
	return false
}

func (query TopMetrics) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
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

		metrics := make(model.JsonMap)
		for _, col := range valuesForMetrics {
			var withoutQuotes string
			if unquoted, err := strconv.Unquote(col.ColName); err == nil {
				withoutQuotes = unquoted
			} else {
				withoutQuotes = col.ColName
			}
			colName, _ := strings.CutPrefix(withoutQuotes, `windowed_`)
			metrics[colName] = col.ExtractValue(query.ctx)
		}
		elem := model.JsonMap{
			"sort":    sortVal,
			"metrics": metrics,
		}
		topElems = append(topElems, elem)
	}
	return model.JsonMap{
		"top": topElems,
	}
}

func (query TopMetrics) String() string {
	return "top_metrics"
}

func (query TopMetrics) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
