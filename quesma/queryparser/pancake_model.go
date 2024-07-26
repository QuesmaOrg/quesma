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
	orderBy  *[]model.OrderByExpr
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
	children []*pancakeAggregationLevel
	orderBy  *[]model.OrderByExpr
	limit    int // 0 if none, only for bucket aggregation
	isKeyed  bool

	metadata model.JsonMap
}

type pancakeAggregation struct {
	// we supported nested aggregation, but one at each level
	bucketAggregations []*pancakeLayerBucketAggregation
	// metric aggregations for each corresponding bucket aggregation
	// 0 - before 0 level of bucket aggregation
	// 1 - after 0 level of bucket aggregation
	metricAggregations [][]*pancakeFillingMetricAggregation

	whereClause model.Expr
}

type pancakeAggregationLayer struct {
	bucketAggregations *pancakeLayerBucketAggregation
	metricAggregations []*pancakeFillingMetricAggregation
}

type pancakeAggregation2 struct {
	layers []pancakeAggregationLayer

	whereClause model.Expr
}
