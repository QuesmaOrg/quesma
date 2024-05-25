package pipeline_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/model"
)

type SerialDiff struct {
	ctx     context.Context
	Parent  string
	IsCount bool
	lag     int
}

func NewSerialDiff(ctx context.Context, bucketsPath string, lag int) SerialDiff {
	isCount := bucketsPath == BucketsPathCount
	return SerialDiff{
		ctx:     ctx,
		Parent:  bucketsPath,
		IsCount: isCount,
		lag:     lag,
	}
}

func (query SerialDiff) IsBucketAggregation() bool {
	return false
}

func (query SerialDiff) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	return translateSqlResponseToJsonCommon(query.ctx, rows, query.String())
}

func (query SerialDiff) CalculateResultWhenMissing(qwa *model.Query, parentRows []model.QueryResultRow) []model.QueryResultRow {
	return calculateResultWhenMissingCommonForDiffAggregations(query.ctx, parentRows, query.lag)
}

func (query SerialDiff) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}

func (query SerialDiff) String() string {
	return fmt.Sprintf("serial_diff(parent: %s, lag: %d)", query.Parent, query.lag)
}
