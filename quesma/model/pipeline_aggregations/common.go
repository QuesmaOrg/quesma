package pipeline_aggregations

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
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

func getKey(ctx context.Context, row model.QueryResultRow, query *model.Query) any {
	if len(row.Cols) < 2 {
		logger.WarnWithCtx(ctx).Msgf("row has less than 2 columns: %v", row)
		return nil
	}
	return row.Cols[len(row.Cols)-2].Value
}
