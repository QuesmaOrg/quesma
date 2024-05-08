package pipeline_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/util"
)

type CumulativeSum struct {
	ctx     context.Context
	Parent  string
	IsCount bool
}

func NewCumulativeSum(ctx context.Context, bucketsPath string) CumulativeSum {
	isCount := bucketsPath == countPath
	return CumulativeSum{ctx: ctx, Parent: bucketsPath, IsCount: isCount}
}

const countPath = "_count"

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
	fmt.Println("hoho")
	resultRow := parentRow.Copy() // result is the same as parent, with an exception of last element, which we'll change below
	parentValue := parentRow.Cols[len(parentRow.Cols)-1].Value
	var resultValue any
	if len(previousResultsCurrentAggregation) == 0 {
		resultValue = parentValue
	} else {
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
	fmt.Println("resultRow", resultRow)
	return resultRow
}

func (query CumulativeSum) String() string {
	return fmt.Sprintf("cumulative_sum(%s)", query.Parent)
}
