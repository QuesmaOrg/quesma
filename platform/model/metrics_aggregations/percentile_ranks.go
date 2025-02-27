// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"strconv"
	"strings"
)

type PercentileRanks struct {
	ctx       context.Context
	cutValues []string // countIf(field<=$cutValue)/count(*)*100
	// defines what response should look like
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-rank-aggregation.html#_keyed_response_5
	Keyed bool
}

func NewPercentileRanks(ctx context.Context, cutValues []string, keyed bool) PercentileRanks {
	return PercentileRanks{ctx: ctx, cutValues: cutValues, Keyed: keyed}
}

func (query PercentileRanks) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query PercentileRanks) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows in percentile ranks response")
		return make(model.JsonMap)
	}
	// I duplicate a lot of code in this if/else below,
	// but I think it's worth it, as this function might get called a lot of times for a single query.
	// And because of complete separation in if/else, I guess it might (should) be slightly faster (?)
	if query.Keyed {
		valueMap := make(model.JsonMap)
		for i, percentileRank := range rows[0].Cols {
			// It always needs to have .Y or .YZ at the end, so 1 or 2 digits after the dot, and dot is mandatory.
			// Also, can't be .00, needs to be .0
			cutValue := query.cutValues[i]
			dot := strings.Index(cutValue, ".")
			if dot == -1 {
				cutValue += ".0"
			} else if dot+len(".00") <= len(cutValue) && cutValue[dot:dot+3] == ".00" {
				cutValue = cutValue[:dot+2]
			} else {
				cutValue = cutValue[:dot+3]
			}
			if value, ok := percentileRank.Value.(float64); ok {
				valueMap[cutValue] = value
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("failed to convert percentile rank value to float64, type: %T, value: %v. Skipping",
					percentileRank.Value, percentileRank.Value)
			}
		}
		return model.JsonMap{
			"values": valueMap,
		}
	} else {
		buckets := make([]model.JsonMap, 0)
		for i, percentileRank := range rows[0].Cols {
			// It always needs to have .Y or .YZ at the end, so 1 or 2 digits after the dot, and dot is mandatory.
			// Also, can't be .00, needs to be .0
			cutValue := query.cutValues[i]
			dot := strings.Index(cutValue, ".")
			if dot == -1 {
				cutValue += ".0"
			} else if dot+len(".00") <= len(cutValue) && cutValue[dot:dot+3] == ".00" {
				cutValue = cutValue[:dot+2]
			} else {
				cutValue = cutValue[:dot+3]
			}
			cutValueFloat, _ := strconv.ParseFloat(cutValue, 64)
			if value, ok := percentileRank.Value.(float64); ok {
				buckets = append(buckets, model.JsonMap{
					"key":   cutValueFloat,
					"value": value,
				})
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("failed to convert percentile rank value to float64, type: %T, value: %v. Skipping",
					percentileRank.Value, percentileRank.Value)
			}
		}
		return model.JsonMap{
			"values": buckets,
		}
	}
}

func (query PercentileRanks) String() string {
	return "percentile_ranks"
}
