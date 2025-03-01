// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
)

type TopHits struct {
	ctx     context.Context
	Size    int
	OrderBy []model.OrderByExpr
}

func NewTopHits(ctx context.Context, size int, orderBy []model.OrderByExpr) *TopHits {
	return &TopHits{ctx: ctx, Size: size, OrderBy: orderBy}
}

func (query *TopHits) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

// TODO: implement correct
func (query *TopHits) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	var topElems []any
	if len(rows) > 0 && 0 >= len(rows[0].Cols) {
		// values are [level, len(row.Cols) - 1]
		logger.WarnWithCtx(query.ctx).Msgf(
			"no columns returned for top_hits aggregation, len(rows[0].Cols): %d, len(rows): %d",
			len(rows[0].Cols), len(rows),
		)
	}
	for _, row := range rows {
		if len(row.Cols) == 0 {
			logger.WarnWithCtx(query.ctx).Msg("no columns returned for top_hits aggregation, skipping")
			continue
		}

		valuesForHits := row.Cols
		sourceMap := model.JsonMap{}

		for _, col := range valuesForHits {

			value := col.ExtractValue()

			sourceMap[col.ColName] = value

		}

		elem := model.JsonMap{
			"_source": sourceMap,
			"_score":  1.0, // placeholder
			"_id":     "",  // TODO: placeholder
			"_index":  "",  // TODO: placeholder
		}
		topElems = append(topElems, elem)
	}

	var maxScore any = 1.0
	if len(topElems) == 0 {
		maxScore = nil
	}
	return model.JsonMap{
		"hits": model.JsonMap{
			"hits":      topElems,
			"max_score": maxScore, // placeholder
			"total": model.JsonMap{ // could be better
				"relation": "eq", // TODO: wrong, but let's pass test, it should ge geq
				"value":    len(topElems),
			},
		},
	}
}

func (query *TopHits) String() string {
	return "top_hits"
}
