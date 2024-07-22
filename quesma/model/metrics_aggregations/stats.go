// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"quesma/logger"
	"quesma/model"
	"strings"
)

type Stats struct {
	ctx context.Context
}

func NewStats(ctx context.Context) Stats {
	return Stats{ctx: ctx}
}

func (query Stats) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query Stats) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for stats aggregation")
		return model.JsonMap{
			"value": nil, // not completely sure if it's a good return value, but it looks fine to me. We should always get 1 row, not 0 anyway.
		}
	}
	if len(rows) > 1 {
		logger.WarnWithCtx(query.ctx).Msgf("more than one row returned for stats aggregation, using only first. rows[0]: %+v, rows[1]: %+v", rows[0], rows[1])
	}

	resultMap := make(model.JsonMap)
	for _, v := range rows[0].Cols[level:] {
		// v.ColName = e.g. avgOrNull(...). We need to extract only 'avg'.
		// first: avgOrNull(..) -> avgOrNull
		firstLeftBracketIndex := strings.Index(v.ColName, "(")
		if firstLeftBracketIndex == -1 {
			logger.Warn().Msgf("invalid column name in stats aggregation: %s. Skipping", v.ColName)
			continue
		}
		fullName := v.ColName[:firstLeftBracketIndex]
		// second: if ends with OrNull, then avgOrNull -> avg
		withoutOrNull, _ := strings.CutSuffix(fullName, "OrNull")
		resultMap[withoutOrNull] = v.Value
	}
	return resultMap
}

func (query Stats) String() string {
	return "stats"
}

func (query Stats) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}
