// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
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
	children []*pancakeAggregationLevel
	orderBy  []model.OrderByExpr
	limit    int // 0 if none, only for bucket aggregation
	isKeyed  bool

	metadata    model.JsonMap
	whereClause model.Expr
}

type pancakeFillingMetricAggregation struct {
	name            string
	queryType       model.QueryType // it has to be metric aggregation
	selectedColumns []model.Expr

	metadata model.JsonMap
}

type pancakeLayerBucketAggregation struct {
	name            string
	queryType       model.QueryType // it has to be bucket aggregation
	selectedColumns []model.Expr

	// only for bucket aggregations
	orderBy []model.OrderByExpr
	limit   int // 0 if none, only for bucket aggregation
	isKeyed bool

	metadata model.JsonMap
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

// Not a real aggregation, but it is a pancake that has alternative JSON rendering
type PancakeQueryType struct {
	pancakeAggregation *pancakeAggregation
}

func (p PancakeQueryType) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) model.JsonMap {
	panic("pancake is not implemented")
}

func (p PancakeQueryType) AggregationType() model.AggregationType {
	return model.TypicalAggregation
}

func (p PancakeQueryType) String() string {
	return "pancake query type"
}
