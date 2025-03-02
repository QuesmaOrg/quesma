// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/util"
	"math"
)

type ExtendedStats struct {
	ctx   context.Context
	sigma float64 // sigma is for std deviation bounds. We need to return (avg +- sigma*stddev) in the response.
}

func NewExtendedStats(ctx context.Context, sigma float64) ExtendedStats {
	return ExtendedStats{ctx: ctx, sigma: sigma}
}

const selectFieldsNr = 10 // how many selects we do to Clickhouse for this aggregation (count, min, ...)

func (query ExtendedStats) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query ExtendedStats) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for stats aggregation")
		return model.JsonMap{
			"value": nil, // not completely sure if it's a good return value, but it looks fine to me. We should always get 1 row, not 0 anyway.
		}
	}
	if len(rows) > 1 {
		logger.WarnWithCtx(query.ctx).Msgf("more than one row returned for stats aggregation, using only first. rows[0]: %+v, rows[1]: %+v", rows[0], rows[1])
	}
	if len(rows[0].Cols) < selectFieldsNr {
		logger.WarnWithCtx(query.ctx).Msgf("not enough fields in the response for extended_stats aggregation. Expected at least %d, got %d. Got: %+v. Returning empty result.", selectFieldsNr, len(rows[0].Cols), rows[0])
		return model.JsonMap{
			"value": nil, // not completely sure if it's a good return value, but it looks fine to me. We should always get >= selectFieldsNr columns anyway.
		}
	}

	row := rows[0]
	var upper, lower, upperSampling, lowerSampling any = "NaN", "NaN", "NaN", "NaN"
	avg, okAvg := util.ExtractNumeric64Maybe(query.getValue(row, "avg"))
	stdDev, okStdDev := util.ExtractNumeric64Maybe(query.getValue(row, "stddev"))
	stdDevSampling, okStdDevSampling := util.ExtractNumeric64Maybe(query.getValue(row, "stddev_sampling"))
	if okAvg && okStdDev {
		upper = avg + query.sigma*stdDev
		lower = avg - query.sigma*stdDev
	}
	if okAvg && okStdDevSampling {
		upperSampling = avg + query.sigma*stdDevSampling
		lowerSampling = avg - query.sigma*stdDevSampling
	}

	return model.JsonMap{
		"count":                    query.getValue(row, "count"),
		"min":                      query.getValue(row, "min"),
		"max":                      query.getValue(row, "max"),
		"avg":                      query.getValue(row, "avg"),
		"sum":                      query.getValue(row, "sum"),
		"sum_of_squares":           query.getValue(row, "sum_of_squares"),
		"variance":                 query.getValue(row, "variance"),
		"variance_population":      query.getValue(row, "variance"),
		"variance_sampling":        query.getValue(row, "variance_sampling"),
		"std_deviation":            query.getValue(row, "stddev"),
		"std_deviation_population": query.getValue(row, "stddev"),
		"std_deviation_sampling":   query.getValue(row, "stddev_sampling"),
		"std_deviation_bounds": model.JsonMap{
			"upper":            upper,
			"lower":            lower,
			"upper_population": upper,
			"lower_population": lower,
			"upper_sampling":   upperSampling,
			"lower_sampling":   lowerSampling,
		},
	}
}

func (query ExtendedStats) String() string {
	return fmt.Sprintf("extended_stats(sigma=%f)", query.sigma)
}

// we're not out of bounds for row.Cols[idx], because we've checked it in TranslateSqlResponseToJson
func (query ExtendedStats) getValue(row model.QueryResultRow, functionName string) any {
	l := len(row.Cols)
	functionNameToColumnIdx := map[string]int{
		"count":             l - 10,
		"min":               l - 9,
		"max":               l - 8,
		"avg":               l - 7,
		"sum":               l - 6,
		"sum_of_squares":    l - 5,
		"variance":          l - 4,
		"variance_sampling": l - 3,
		"stddev":            l - 2,
		"stddev_sampling":   l - 1,
	}
	column, ok := functionNameToColumnIdx[functionName]
	if !ok {
		logger.WarnWithCtx(query.ctx).Msgf("unknown function name: %s, row: %+v", functionName, row)
		return nil
	}

	valueAsFloat, isFloat := row.Cols[column].Value.(float64)
	if row.Cols[column].Value == nil || (isFloat && math.IsNaN(valueAsFloat)) {
		return "NaN"
	}
	return row.Cols[column].Value
}

func (query ExtendedStats) ColumnIdx(name string) int {
	nameToColumnIdx := map[string]int{
		"count":                    0,
		"min":                      1,
		"max":                      2,
		"avg":                      3,
		"sum":                      4,
		"sum_of_squares":           5,
		"variance":                 6,
		"variance_population":      6,
		"variance_sampling":        7,
		"std_deviation":            8,
		"std_deviation_population": 8,
		"std_deviation_sampling":   9,
	}

	if columnIdx, ok := nameToColumnIdx[name]; ok {
		return columnIdx
	}
	logger.ErrorWithCtx(query.ctx).Msgf("extended_stats column %s not found", name)
	return -1
}
