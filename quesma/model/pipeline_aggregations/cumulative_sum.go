package pipeline_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/util"
)

// We fully support this aggregation.
// Description: A parent pipeline aggregation which calculates the cumulative sum of a specified metric
// in a parent histogram (or date_histogram) aggregation.
// The specified metric must be numeric and the enclosing histogram must have min_doc_count set to 0 (default for histogram aggregations).
// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-cumulative-sum-aggregation.html

type CumulativeSum struct {
	ctx     context.Context
	Parent  string
	IsCount bool // count is a special case, `bucketsPath` is not a path to another aggregation, but path-to-aggregation>_count
}

func NewCumulativeSum(ctx context.Context, bucketsPath string) CumulativeSum {
	isCount := bucketsPath == bucketsPathCount
	return CumulativeSum{ctx: ctx, Parent: bucketsPath, IsCount: isCount}
}

const bucketsPathCount = "_count" // special name for `buckets_path` parameter, normally it's some other aggregation's name

func (query CumulativeSum) IsBucketAggregation() bool {
	return false
}

func (query CumulativeSum) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for cumulative sum aggregation")
		return []model.JsonMap{{}}
	}
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{"value": row.Cols[len(row.Cols)-1].Value})
	}
	return response
}

func (query CumulativeSum) CalculateResultWhenMissing(parentRow model.QueryResultRow, previousResultsCurrentAggregation []model.QueryResultRow) model.QueryResultRow {
	resultRow := parentRow.Copy() // result is the same as parent, with an exception of last element, which we'll change below
	parentValue := parentRow.Cols[len(parentRow.Cols)-1].Value
	var resultValue any
	if len(previousResultsCurrentAggregation) == 0 {
		resultValue = parentValue
	} else {
		// I don't check types too much, they are expected to be numeric, so either floats or ints.
		// I propose to keep it this way until at least one case arises as this method can be called a lot of times.
		previousValue := previousResultsCurrentAggregation[len(previousResultsCurrentAggregation)-1].Cols[len(previousResultsCurrentAggregation[len(previousResultsCurrentAggregation)-1].Cols)-1].Value
		parentValueAsFloat, ok := util.ExtractFloat64Maybe(parentValue)
		if ok {
			previousValueAsFloat, ok := util.ExtractFloat64Maybe(previousValue)
			if ok {
				resultValue = parentValueAsFloat + previousValueAsFloat
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert previous value to float: %v, parentValue: %v", previousValue, parentValue)
				resultValue = previousValue
			}
		} else {
			previousValueAsInt := util.ExtractInt64(previousValue)
			parentValueAsInt := util.ExtractInt64(parentValue)
			resultValue = parentValueAsInt + previousValueAsInt
		}
	}
	resultRow.Cols[len(resultRow.Cols)-1].Value = resultValue
	return resultRow
}

func (query CumulativeSum) String() string {
	return fmt.Sprintf("cumulative_sum(%s)", query.Parent)
}
