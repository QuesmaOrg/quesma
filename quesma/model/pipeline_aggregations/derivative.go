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
	isCount := bucketsPath == BucketsPathCount
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

func (query Derivative) CalculateResultWhenMissing(qwa *model.Query, parentRows []model.QueryResultRow) []model.QueryResultRow {
	resultRows := make([]model.QueryResultRow, 0, len(parentRows))
	if len(parentRows) == 0 {
		return resultRows
	}

	firstRow := parentRows[0].Copy()
	firstRow.Cols[len(firstRow.Cols)-1].Value = nil
	resultRows = append(resultRows, firstRow)
	if _, firstRowValueIsFloat := util.ExtractFloat64Maybe(parentRows[0].LastColValue()); firstRowValueIsFloat {
		for i, currentRow := range parentRows[1:] {
			previousRow := parentRows[i]
			previousValueRaw := previousRow.LastColValue()
			previousValue, okPrevious := util.ExtractFloat64Maybe(previousValueRaw)

			currentValueRaw := currentRow.LastColValue()
			currentValue, okCurrent := util.ExtractFloat64Maybe(currentValueRaw)

			var resultValue any
			if okPrevious && okCurrent {
				resultValue = currentValue - previousValue
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: previousValue: %v, type: %T; currentValue: %v, type: %T. Skipping",
					previousValueRaw, previousValueRaw, currentValueRaw, currentValueRaw)
				resultValue = nil
			}
			resultRow := currentRow.Copy()
			resultRow.Cols[len(resultRow.Cols)-1].Value = resultValue
			resultRows = append(resultRows, resultRow)
		}
	} else { // cumulative sum must be on numeric, so if it's not float64, it should always be int
		for i, currentRow := range parentRows[1:] {
			previousRow := parentRows[i]
			previousValueRaw := previousRow.LastColValue()
			previousValue, okPrevious := util.ExtractInt64Maybe(previousValueRaw)

			currentValueRaw := currentRow.LastColValue()
			currentValue, okCurrent := util.ExtractInt64Maybe(currentValueRaw)

			var resultValue any
			if okPrevious && okCurrent {
				resultValue = currentValue - previousValue
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to int: previousValue: %v, type: %T; currentValue: %v, type: %T. Skipping",
					previousValueRaw, previousValueRaw, currentValueRaw, currentValueRaw)
				resultValue = nil
			}
			resultRow := currentRow.Copy()
			resultRow.Cols[len(resultRow.Cols)-1].Value = resultValue
			resultRows = append(resultRows, resultRow)
		}
	}
	return resultRows
}

func (query Derivative) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}

func (query Derivative) String() string {
	return fmt.Sprintf("derivative(%s)", query.Parent)
}
