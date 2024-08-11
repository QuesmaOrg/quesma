// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"quesma/model"
)

// Pancake model.
// Each bucket aggregation adds new layer, in between there could be many metric aggregations.
type pancakeModel struct {
	layers []*pancakeModelLayer
	// invariant: len(layers) > 0 && layers[len(layers)-1].nextBucketAggregation == nil

	whereClause model.Expr
	sampleLimit int
}

type pancakeModelLayer struct {
	nextBucketAggregation     *pancakeModelBucketAggregation
	currentMetricAggregations []*pancakeModelMetricAggregation
}

type pancakeModelMetricAggregation struct {
	name            string          // as originally appeared in Query DSL
	internalName    string          // full name with path, e.g. metric__byCountry__byCity__population or aggr__byCountry
	queryType       model.QueryType // it has to be metric aggregation
	selectedColumns []model.Expr

	metadata model.JsonMap
}

type pancakeModelBucketAggregation struct {
	name            string          // as originally appeared in Query DSL
	internalName    string          // full name with path, e.g. metric__byCountry__byCity__population or aggr__byCountry
	queryType       model.QueryType // it has to be bucket aggregation
	selectedColumns []model.Expr

	// only for bucket aggregations
	orderBy []model.OrderByExpr
	limit   int // pancakeBucketAggregationNoLimit if none
	isKeyed bool

	metadata                model.JsonMap
	filterOurEmptyKeyBucket bool
}

const pancakeBucketAggregationNoLimit = 0
const noSampleLimit = 0

// Helper functions
func (p pancakeModelBucketAggregation) InternalNameForKeyPrefix() string {
	return fmt.Sprintf("%skey", p.internalName)
}

func (p pancakeModelBucketAggregation) InternalNameForKey(id int) string {
	return fmt.Sprintf("%s_%d", p.InternalNameForKeyPrefix(), id)
}

func (p pancakeModelBucketAggregation) InternalNameForOrderBy(id int) string {
	return fmt.Sprintf("%sorder_%d", p.internalName, id)
}

func (p pancakeModelBucketAggregation) InternalNameForCount() string {
	return fmt.Sprintf("%scount", p.internalName)
}
