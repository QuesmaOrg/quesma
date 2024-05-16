package pipeline_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/util"
)

type Derivative struct {
	ctx     context.Context
	Parent  string
	IsCount bool
}

func NewDerivative(ctx context.Context, bucketsPath string) Derivative {
	isCount := bucketsPath == bucketsPathCount
	return Derivative{ctx: ctx, Parent: bucketsPath, IsCount: isCount}
}

func (query Derivative) IsBucketAggregation() bool {
	return false
}

func (query Derivative) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for derivative aggregation")
		return []model.JsonMap{{}}
	}
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{"value": row.Cols[len(row.Cols)-1].Value})
	}
	return response
}

func (query Derivative) CalculateResultWhenMissing(rowIndex int, parentRows []model.QueryResultRow, previousResultsCurrentAggregation []model.QueryResultRow) model.QueryResultRow {
	resultRow := parentRows[rowIndex].Copy() // result is the same as parent, with an exception of last element, which we'll change below
	var resultValue any
	if rowIndex == 0 {
		resultValue = nil
	} else {
		previousValue := parentRows[rowIndex-1].Cols[len(parentRows[rowIndex-1].Cols)-1].Value
		currentValue := parentRows[rowIndex].Cols[len(parentRows[rowIndex].Cols)-1].Value
		currentValueAsFloat, ok := util.ExtractFloat64Maybe(currentValue)
		if ok {
			previousValueAsFloat, ok := util.ExtractFloat64Maybe(previousValue)
			if ok {
				resultValue = currentValueAsFloat - previousValueAsFloat
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert previous value to float: %v, currentValue: %v", previousValue, currentValue)
				resultValue = nil
			}
		} else {
			previousValueAsInt, okPrevious := util.ExtractInt64Maybe(previousValue)
			currentValueAsInt, okParent := util.ExtractInt64Maybe(currentValue)
			if okPrevious && okParent {
				resultValue = currentValueAsInt - previousValueAsInt
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert previous or current value to int, previousValue: %v, currentValue: %v. Using nil as result", previousValue, currentValue)
				resultValue = nil
			}
		}
	}
	resultRow.Cols[len(resultRow.Cols)-1].Value = resultValue
	return resultRow
}

func (query Derivative) String() string {
	return fmt.Sprintf("derivative(%s)", query.Parent)
}
