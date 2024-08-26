package pipeline_aggregations

import (
	"context"
	"quesma/logger"
	"strings"
)

const BucketsPathCount = "_count" // special name for `buckets_path` parameter, normally it's some other aggregation's name

type PipelineAggregation struct {
	// TODO change name to ParentMetricAggregationName? Not 100% sure it must be metric.
	// Deprecated: used only in old logic, can be removed later.
	Parent       string
	PathToParent []string
	isCount      bool // count is a special case, `bucketsPath` is not a path to another aggregation, but path-to-aggregation>_count
}

func newPipelineAggregation(ctx context.Context, bucketsPath string) PipelineAggregation {
	const delimiter = ">"
	if len(bucketsPath) == 0 {
		logger.WarnWithCtx(ctx).Msgf("invalid bucketsPath: %s. Using empty string as parent.", bucketsPath)
		return PipelineAggregation{}
	}

	parent := ""
	withoutCountSuffix, _ := strings.CutSuffix(bucketsPath, delimiter+BucketsPathCount) // remove _count suffix
	lastDelimiterIdx := strings.LastIndex(withoutCountSuffix, delimiter)
	if lastDelimiterIdx+1 < len(withoutCountSuffix) {
		parent = withoutCountSuffix[lastDelimiterIdx+1:]
	}

	isCount := bucketsPath == BucketsPathCount || strings.HasSuffix(bucketsPath, delimiter+BucketsPathCount)
	splitPath := strings.Split(bucketsPath, delimiter)
	return PipelineAggregation{Parent: parent, PathToParent: splitPath[:len(splitPath)-1], isCount: isCount}
}

func (p PipelineAggregation) GetParent() string {
	return p.Parent
}

func (p PipelineAggregation) GetPathToParent() []string {
	return p.PathToParent
}

func (p PipelineAggregation) IsCount() bool {
	return p.isCount
}
