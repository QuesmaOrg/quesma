// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"github.com/k0kubun/pp"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"strings"
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
	nextBucketAggregation       *pancakeModelBucketAggregation
	currentMetricAggregations   []*pancakeModelMetricAggregation
	currentPipelineAggregations []*pancakeModelPipelineAggregation

	childrenPipelineAggregations []*pancakeModelPipelineAggregation
}

func newPancakeModelLayer() *pancakeModelLayer {
	return &pancakeModelLayer{
		currentMetricAggregations:    make([]*pancakeModelMetricAggregation, 0),
		currentPipelineAggregations:  make([]*pancakeModelPipelineAggregation, 0),
		childrenPipelineAggregations: make([]*pancakeModelPipelineAggregation, 0),
	}
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

type pancakeModelPipelineAggregation struct {
	name            string // as originally appeared in Query DSL
	internalName    string // full name with path, e.g. metric__byCountry__byCity__population or aggr__byCountry
	queryType       model.PipelineQueryType
	selectedColumns []model.Expr

	metadata model.JsonMap
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

// Used by terms aggregation to get the total count, so we can calculate sum_other_doc_count
func (p pancakeModelBucketAggregation) InternalNameForParentCount() string {
	return fmt.Sprintf("%sparent_count", p.internalName)
}

func (p pancakeModelBucketAggregation) DoesHaveGroupBy() bool {
	_, noGroupBy := p.queryType.(bucket_aggregations.NoGroupByInterface)
	return !noGroupBy
}

func (p pancakeModelPipelineAggregation) parentColumnName(ctx context.Context) string {
	// At start p.internalName = e.g. pipeline__2__1
	prefix := strings.TrimSuffix(p.internalName, p.name) // First remove this aggregation name (1)
	suffix := strings.Join(append(p.queryType.GetPathToParent(), p.queryType.GetParent()), "__") + "_col_0"
	fullPath := prefix + suffix
	if !strings.HasPrefix(fullPath, "pipeline") {
		logger.WarnWithCtx(ctx).Msgf("prefix %s does not start with 'pipeline'", fullPath)
		return ""
	}
	return "metric" + fullPath[8:]
}

func (p *pancakeModelLayer) findPipelineChildren(pipeline *pancakeModelPipelineAggregation) []*pancakeModelPipelineAggregation {
	var result []*pancakeModelPipelineAggregation
	for _, child := range p.childrenPipelineAggregations {
		pp.Println("child.queryType.GetParent()", child.queryType.GetParent(), pipeline.name, pipeline.internalName, child.internalName)
		if child.queryType.GetParent() == pipeline.name {
			result = append(result, child)
		}
	}
	pp.Println("findPipelineChildren, pipeline:", pipeline.internalName, "result:", result)
	return result
}
