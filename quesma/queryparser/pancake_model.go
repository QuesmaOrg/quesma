// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"quesma/logger"
	"quesma/model"
)

type pancakeAggregationTopLevel struct {
	children    []*pancakeAggregationLevel
	whereClause model.Expr
}

type pancakeAggregationLevel struct {
	name            string
	queryType       model.QueryType
	selectedColumns []model.Expr

	// only for bucket aggregations
	children                []*pancakeAggregationLevel
	orderBy                 []model.OrderByExpr
	limit                   int // 0 if none, only for bucket aggregation
	isKeyed                 bool
	filterOutEmptyKeyBucket bool

	metadata model.JsonMap
}

type pancakeFillingMetricAggregation struct {
	name            string          // as originally appeared in Query DSL
	aliasName       string          // full name with path, e.g. metric__byCountry__byCity__population or aggr__byCountry
	queryType       model.QueryType // it has to be metric aggregation
	selectedColumns []model.Expr

	metadata model.JsonMap
}

type pancakeLayerBucketAggregation struct {
	name            string          // as originally appeared in Query DSL
	aliasName       string          // full name with path, e.g. metric__byCountry__byCity__population or aggr__byCountry
	queryType       model.QueryType // it has to be bucket aggregation
	selectedColumns []model.Expr

	// only for bucket aggregations
	orderBy []model.OrderByExpr
	limit   int // 0 if none, only for bucket aggregation
	isKeyed bool

	metadata                model.JsonMap
	filterOurEmptyKeyBucket bool
}

type pancakeAggregationLayer struct {
	nextBucketAggregation     *pancakeLayerBucketAggregation
	currentMetricAggregations []*pancakeFillingMetricAggregation
}

type pancakeAggregation struct {
	layers []*pancakeAggregationLayer
	// invariant: len(layers) > 0 && layers[len(layers)-1].nextBucketAggregation == nil

	whereClause model.Expr
}

const PancakeTotalCountMetricName = "__quesma_total_count"

// Not a real aggregation, but it is a pancake that has alternative JSON rendering
type PancakeQueryType struct {
	pancakeAggregation *pancakeAggregation
}

func (p PancakeQueryType) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	panic("not a real aggregation, it should not be never used")
}

func (p PancakeQueryType) AggregationType() model.AggregationType {
	return model.TypicalAggregation
}

func (p PancakeQueryType) String() string {
	return "pancake query type"
}

func (p PancakeQueryType) ReturnTotalCount() *pancakeFillingMetricAggregation {

	if len(p.pancakeAggregation.layers) > 0 {
		for _, metric := range p.pancakeAggregation.layers[0].currentMetricAggregations {
			if metric.name == PancakeTotalCountMetricName {
				return metric
			}
		}
	}

	return nil
}

func (p PancakeQueryType) RenderAggregationJson(rows []model.QueryResultRow) (model.JsonMap, error) {
	renderer := &pancakeJSONRenderer{}
	res, err := renderer.toJSON(p.pancakeAggregation, rows)
	if err != nil {
		logger.Error().Err(err).Msg("Error rendering JSON. Returning empty.")
	}
	return res, err
}
