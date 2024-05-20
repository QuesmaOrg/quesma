package pipeline_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"strings"
)

func parseBucketsPathIntoParentAggregationName(ctx context.Context, bucketsPath string) (parentAggregationName string) {
	const delimiter = ">"
	withoutUnnecessarySuffix, _ := strings.CutSuffix(bucketsPath, delimiter+BucketsPathCount)
	lastDelimiterIdx := strings.LastIndex(withoutUnnecessarySuffix, delimiter)
	if lastDelimiterIdx+1 < len(withoutUnnecessarySuffix) {
		parentAggregationName = withoutUnnecessarySuffix[lastDelimiterIdx+1:]
	} else {
		logger.WarnWithCtx(ctx).Msgf("invalid bucketsPath: %s, withoutUnnecessarySuffix: %s. Using empty string as parent.", bucketsPath, withoutUnnecessarySuffix)
		parentAggregationName = ""
	}
	return
}
