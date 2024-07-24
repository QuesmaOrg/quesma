// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import "quesma/model"

type aggregationTopLevelVersionUna struct {
	children    []*aggregationLevelVersionUna
	whereClause model.Expr
}

type aggregationLevelVersionUna struct {
	name            string
	queryType       model.QueryType
	selectedColumns []model.Expr

	// only for bucket aggregations
	children []*aggregationLevelVersionUna
	orderBy  *[]model.OrderByExpr
	limit    int // 0 if none, only for bucket aggregation
	isKeyed  bool

	metadata    model.JsonMap
	whereClause model.Expr
}

type metricAggregationPancakeFillingVersionUna struct {
	name            string
	queryType       model.QueryType // it has to be metric aggregation
	selectedColumns []model.Expr

	metadata model.JsonMap
}

type bucketAggregationPancakeLayerVersionUna struct {
	name            string
	queryType       model.QueryType // it has to be bucket aggregation
	selectedColumns []model.Expr

	// only for bucket aggregations
	children []*aggregationLevelVersionUna
	orderBy  *[]model.OrderByExpr
	limit    int // 0 if none, only for bucket aggregation
	isKeyed  bool

	metadata model.JsonMap
}

type aggregationPancakeVersionUna struct {
	// we supported nested aggregation, but one at each level
	bucketAggregations []*bucketAggregationPancakeLayerVersionUna
	// metric aggregations for each corresponding bucket aggregation
	// 0 - before 0 level of bucket aggregation
	// 1 - after 0 level of bucket aggregation
	metricAggregations [][]*metricAggregationPancakeFillingVersionUna

	whereClause model.Expr
}
