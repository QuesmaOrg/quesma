// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/model/bucket_aggregations"
	"strings"
)

const BucketsPathCount = "_count" // special name for `buckets_path` parameter, normally it's some other aggregation's name

type PipelineAggregation struct {
	ctx context.Context
	// Deprecated: used only in old logic, can be removed later.
	Parent       string
	PathToParent []string
	// May be nil, as there may be no parent bucket aggregation.
	// Also, it's always nil at start (after constructor), it's set later during pancake transformation.
	parentBucketAggregation model.QueryType
	isCount                 bool // count is a special case, `bucketsPath` is not a path to another aggregation, but path-to-aggregation>_count
}

func newPipelineAggregation(ctx context.Context, bucketsPath string) *PipelineAggregation {
	const delimiter = ">"
	if len(bucketsPath) == 0 {
		logger.WarnWithCtx(ctx).Msgf("invalid bucketsPath: %s. Using empty string as parent.", bucketsPath)
		return &PipelineAggregation{isCount: true} // count, as it's the simplest case
	}

	parent := ""
	withoutCountSuffix, _ := strings.CutSuffix(bucketsPath, delimiter+BucketsPathCount) // remove _count suffix
	lastDelimiterIdx := strings.LastIndex(withoutCountSuffix, delimiter)
	if lastDelimiterIdx+1 < len(withoutCountSuffix) {
		parent = withoutCountSuffix[lastDelimiterIdx+1:]
	}

	isCount := bucketsPath == BucketsPathCount || strings.HasSuffix(bucketsPath, delimiter+BucketsPathCount)
	splitPath := strings.Split(bucketsPath, delimiter)
	return &PipelineAggregation{ctx: ctx, Parent: parent, PathToParent: splitPath[:len(splitPath)-1], isCount: isCount}
}

func (p *PipelineAggregation) GetParent() string {
	return p.Parent
}

func (p *PipelineAggregation) GetPathToParent() []string {
	return p.PathToParent
}

func (p *PipelineAggregation) IsCount() bool {
	return p.isCount
}

func (p *PipelineAggregation) GetParentBucketAggregation() model.QueryType {
	return p.parentBucketAggregation
}

func (p *PipelineAggregation) SetParentBucketAggregation(parentBucketAggregation model.QueryType) {
	p.parentBucketAggregation = parentBucketAggregation
}

func (p *PipelineAggregation) getKey(row model.QueryResultRow) any {
	if len(row.Cols) < 2 {
		logger.WarnWithCtx(p.ctx).Msgf("row has less than 2 columns: %v", row)
		return nil
	}
	if dateHistogram, ok := p.parentBucketAggregation.(*bucket_aggregations.DateHistogram); ok {
		return dateHistogram.OriginalKeyToKeyAsString(row.Cols[len(row.Cols)-2].Value)
	}
	return row.Cols[len(row.Cols)-2].Value
}
