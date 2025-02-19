// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
)

const PancakeTotalCountMetricName = "__quesma_total_count"

// Not a real aggregation, but it is a pancake that has alternative JSON rendering
type PancakeQueryType struct {
	pancakeAggregation *pancakeModel
}

func (p PancakeQueryType) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	panic("not a real aggregation, it should not be never used")
}

func (p PancakeQueryType) AggregationType() model.AggregationType {
	return model.TypicalAggregation
}

func (p PancakeQueryType) String() string {
	return "pancake query type"
}

func (p PancakeQueryType) ReturnTotalCount() *pancakeModelMetricAggregation {

	if len(p.pancakeAggregation.layers) > 0 {
		for _, metric := range p.pancakeAggregation.layers[0].currentMetricAggregations {
			if metric.name == PancakeTotalCountMetricName {
				return metric
			}
		}
	}

	return nil
}

func (p PancakeQueryType) RenderAggregationJson(ctx context.Context, rows []model.QueryResultRow) (model.JsonMap, error) {
	renderer := newPancakeJSONRenderer(ctx)
	res, err := renderer.toJSON(p.pancakeAggregation, rows)
	if err != nil {
		logger.Error().Err(err).Msg("Error rendering JSON. Returning empty.")
	}
	return res, err
}
