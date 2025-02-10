// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"strconv"
	"strings"
)

type TopMetrics struct {
	ctx       context.Context
	Size      int
	SortBy    string
	SortOrder model.OrderByDirection
}

func NewTopMetrics(ctx context.Context, size int, sortBy string, sortOrder string) *TopMetrics {
	var order model.OrderByDirection
	switch sortOrder {
	case "asc":
		order = model.AscOrder
	case "desc":
		order = model.DescOrder
	default:
		logger.WarnWithCtx(ctx).Msgf("invalid sort order: %s, defaulting to desc", sortOrder)
		order = model.DescOrder
	}
	return &TopMetrics{ctx: ctx, Size: size, SortBy: sortBy, SortOrder: order}
}

func (query *TopMetrics) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query *TopMetrics) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	var topElems []any
	if len(rows) > 0 && 0 >= len(rows[0].Cols)-1 {
		logger.WarnWithCtx(query.ctx).Msgf(
			"no columns returned for top_metrics aggregation, len(rows[0].Cols): %d, len(rows): %d",
			len(rows[0].Cols), len(rows),
		)
	}
	for _, row := range rows {
		if len(row.Cols) == 0 {
			logger.WarnWithCtx(query.ctx).Msg("no columns returned for top_metrics aggregation, skipping")
			continue
		}

		var sortVal []any
		var valuesForMetrics []model.QueryResultCol
		if len(query.SortBy) > 0 {
			// per convention, we know that value we sorted by is in the last column (if it exists)
			lastIndex := len(row.Cols) - 1 // last column is the sort column, we don't return it
			sortVal = append(sortVal, row.Cols[lastIndex].Value)
			valuesForMetrics = row.Cols[:lastIndex]
		} else {
			valuesForMetrics = row.Cols
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
			metrics[colName] = col.ExtractValue()
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

func (query *TopMetrics) String() string {
	return "top_metrics"
}
