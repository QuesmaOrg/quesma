package pipeline_aggregations

import (
	"context"
	"quesma/logger"
	"strings"
)

//const BucketsPathCount = "_count" // special name for `buckets_path` parameter, normally it's some other aggregation's name

type PipelineAggregationInterface interface {
	GetParent() string
	GetPathToParent() []string
}

type PipelineAggregation struct {
	Parent       string // TODO change name to ParentMetricAggregationName? Not 100% sure it must be metric.
	PathToParent []string
	IsCount      bool // count is a special case, `bucketsPath` is not a path to another aggregation, but path-to-aggregation>_count
}

func newPipelineAggregation(ctx context.Context, bucketsPath string) PipelineAggregation {
	const delimiter = ">"
	if len(bucketsPath) == 0 {
		logger.WarnWithCtx(ctx).Msgf("invalid bucketsPath: %s. Using empty string as parent.", bucketsPath)
		return PipelineAggregation{}
	}

	isCount := (bucketsPath == BucketsPathCount || strings.HasSuffix(bucketsPath, delimiter+BucketsPathCount))
	splitPath := strings.Split(bucketsPath, delimiter)
	return PipelineAggregation{Parent: splitPath[len(splitPath)-1], PathToParent: splitPath[:len(splitPath)-1], IsCount: isCount}
}

func (p PipelineAggregation) GetParent() string {
	return p.Parent
}

func (p PipelineAggregation) GetPathToParent() []string {
	return p.PathToParent
}
