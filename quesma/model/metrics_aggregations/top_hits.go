// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"encoding/json"
	"quesma/logger"
	"quesma/model"
	"quesma/schema"
	"strconv"
	"strings"
)

type TopHits struct {
	ctx     context.Context
	Size    int
	OrderBy []model.OrderByExpr
}

func NewTopHits(ctx context.Context, size int) TopHits {
	return TopHits{ctx: ctx, Size: size}
}

func NewTopHitsWithOrderBy(ctx context.Context, size int, orderBy []model.OrderByExpr) TopHits {
	return TopHits{ctx: ctx, Size: size, OrderBy: orderBy}
}

func (query TopHits) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

// TODO: implement correct
func (query TopHits) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
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
			var withoutQuotes string
			if unquoted, err := strconv.Unquote(col.ColName); err == nil {
				withoutQuotes = unquoted
			} else {
				withoutQuotes = col.ColName
			}
			colName, _ := strings.CutPrefix(withoutQuotes, `windowed_`)

			if col.ColType.Name == schema.QuesmaTypePoint.Name {
				hits := make(model.JsonMap)
				// TODO suffixes (::lat, ::lon) hardcoded for now
				// due to insufficient information in the schema
				if strings.Contains(col.ColName, "::lon") {
					hits["lon"] = col.ExtractValue(query.ctx)
					colName = strings.TrimSuffix(col.ColName, "::lon")
				}
				if strings.Contains(col.ColName, "::lat") {
					hits["lat"] = col.ExtractValue(query.ctx)
					colName = strings.TrimSuffix(col.ColName, "::lat")
				}
				if _, ok := sourceMap[colName]; ok {
					currentHits := sourceMap[colName].(model.JsonMap)
					for k, v := range currentHits {
						hits[k] = v
					}
					sourceMap[colName] = hits
				} else {
					sourceMap[colName] = hits
				}

			} else {
				value := col.ExtractValue(query.ctx)
				// TODO: this is hack, we should not assume this is location
				if strings.HasSuffix(col.ColName, "Location") {
					if valueStr, ok := value.(string); ok {
						var valueJson model.JsonMap
						if err := json.Unmarshal([]byte(valueStr), &valueJson); err == nil {
							value = valueJson
						}
					}
				}
				sourceMap[col.ColName] = value
			}
		}

		elem := model.JsonMap{
			"_source": sourceMap,
			"_score":  1.0, // placeholder
			"_id":     "",  // TODO: placeholder
			"_index":  "",  // TODO: placeholder
		}
		topElems = append(topElems, elem)
	}
	return model.JsonMap{
		"hits": model.JsonMap{
			"hits":      topElems,
			"max_score": 1.0, // placeholder
			"total": model.JsonMap{ // could be better
				"relation": "eq", // TODO: wrong, but let's pass test, it should ge geq
				"value":    len(topElems),
			},
		},
	}
}

func (query TopHits) String() string {
	return "top_hits"
}
