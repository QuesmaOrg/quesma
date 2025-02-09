// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/model/bucket_aggregations"
	"regexp"
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

// Clone isn't a shallow copy, isn't also a full deep copy, but it's enough for our purposes.
func (p *pancakeModel) Clone() *pancakeModel {
	layers := make([]*pancakeModelLayer, len(p.layers))
	for i, layer := range p.layers {
		layers[i] = newPancakeModelLayer(layer.nextBucketAggregation)
		layers[i].currentMetricAggregations = p.layers[i].currentMetricAggregations
		layers[i].currentPipelineAggregations = p.layers[i].currentPipelineAggregations
		layers[i].childrenPipelineAggregations = p.layers[i].childrenPipelineAggregations
	}
	return &pancakeModel{
		layers:      layers,
		whereClause: p.whereClause,
		sampleLimit: p.sampleLimit,
	}
}

type pancakeModelLayer struct {
	nextBucketAggregation       *pancakeModelBucketAggregation
	currentMetricAggregations   []*pancakeModelMetricAggregation
	currentPipelineAggregations []*pancakeModelPipelineAggregation

	childrenPipelineAggregations []*pancakeModelPipelineAggregation
}

func newPancakeModelLayer(nextBucketAggregation *pancakeModelBucketAggregation) *pancakeModelLayer {
	return &pancakeModelLayer{
		nextBucketAggregation:        nextBucketAggregation,
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
	name         string // as originally appeared in Query DSL
	internalName string // full name with path, e.g. pipeline__byCountry__byCity__population

	// full name with path, e.g. metric__byCountry__byCity__population
	// (at least for now it's always metric - not 100% sure it's the only possibility)
	parentInternalName string
	queryType          model.PipelineQueryType

	metadata model.JsonMap
}

func newPancakeModelPipelineAggregation(name string, previousAggrNames []string, pipelineQueryType model.PipelineQueryType,
	metadata model.JsonMap) *pancakeModelPipelineAggregation {

	internalNamePrefix := strings.Join(previousAggrNames, "__")
	internalName := fmt.Sprintf("%s__%s", internalNamePrefix, name)
	parentInternalName := fmt.Sprintf("metric__%s__%s_col_0", internalNamePrefix,
		strings.Join(append(pipelineQueryType.GetPathToParent(), pipelineQueryType.GetParent()), "__"))

	return &pancakeModelPipelineAggregation{
		name:               name,
		internalName:       internalName,
		parentInternalName: parentInternalName,
		queryType:          pipelineQueryType,
		metadata:           metadata,
	}
}

const pancakeBucketAggregationNoLimit = 0
const noSampleLimit = 0

// Naming functions
func (p pancakeModelMetricAggregation) InternalNamePrefix() string {
	return p.internalName + "_col_"
}

func (p pancakeModelMetricAggregation) InternalNameForCol(id int) string {
	return fmt.Sprintf("%s%d", p.InternalNamePrefix(), id)
}

func (p pancakeModelBucketAggregation) ShallowClone() pancakeModelBucketAggregation {
	return pancakeModelBucketAggregation{
		name:                    p.name,
		internalName:            p.internalName,
		queryType:               p.queryType,
		selectedColumns:         p.selectedColumns,
		orderBy:                 p.orderBy,
		limit:                   p.limit,
		isKeyed:                 p.isKeyed,
		metadata:                p.metadata,
		filterOurEmptyKeyBucket: p.filterOurEmptyKeyBucket,
	}
}

func (p pancakeModelBucketAggregation) InternalNameForKeyPrefix() string {
	return fmt.Sprintf("%skey", p.internalName)
}

func (p pancakeModelBucketAggregation) InternalNameForKey(id int) string {
	return fmt.Sprintf("%s_%d", p.InternalNameForKeyPrefix(), id)
}

func (p pancakeModelBucketAggregation) InternalNameForOrderBy(id int) string {
	return fmt.Sprintf("%sorder_%d", p.internalName, id)
}

func (p pancakeModelBucketAggregation) isInternalNameOrderByColumn(internalName string) bool {
	matched, _ := regexp.MatchString(`.*order_[0-9]+`, internalName)
	return matched
}

func (p pancakeModelBucketAggregation) InternalNameForCount() string {
	return fmt.Sprintf("%scount", p.internalName)
}

// Used by terms aggregation to get the total count, so we can calculate sum_other_doc_count
func (p pancakeModelBucketAggregation) InternalNameForParentCount() string {
	return fmt.Sprintf("%sparent_count", p.internalName)
}

func (p pancakeModelBucketAggregation) isInternalNameCountColumn(internalName string) bool {
	return strings.HasSuffix(internalName, "count")
}

func (p pancakeModelBucketAggregation) DoesHaveGroupBy() bool {
	_, noGroupBy := p.queryType.(bucket_aggregations.NoGroupByInterface)
	return !noGroupBy
}

func (p *pancakeModelLayer) findPipelineChildren(pipeline *pancakeModelPipelineAggregation) []*pancakeModelPipelineAggregation {
	children := make([]*pancakeModelPipelineAggregation, 0)
	for _, maybeChild := range p.childrenPipelineAggregations {
		if maybeChild.queryType.GetParent() == pipeline.name {
			children = append(children, maybeChild)
		}
	}
	return children
}
