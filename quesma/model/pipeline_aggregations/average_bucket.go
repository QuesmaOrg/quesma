package pipeline_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryprocessor"
	"mitmproxy/quesma/util"
)

type AverageBucket struct {
	ctx    context.Context
	Parent string
}

func NewAverageBucket(ctx context.Context, bucketsPath string) AverageBucket {
	return AverageBucket{ctx: ctx, Parent: parseBucketsPathIntoParentAggregationName(ctx, bucketsPath)}
}

func (query AverageBucket) IsBucketAggregation() bool {
	return false
}

func (query AverageBucket) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for average bucket aggregation")
		return []model.JsonMap{{}}
	}
	var response []model.JsonMap
	for _, row := range rows {
		response = append(response, model.JsonMap{"value": row.Cols[len(row.Cols)-1].Value})
	}
	return response
}

func (query AverageBucket) CalculateResultWhenMissing(qwa *model.Query, parentRows []model.QueryResultRow) []model.QueryResultRow {
	resultRows := make([]model.QueryResultRow, 0)
	if len(parentRows) == 0 {
		return resultRows // maybe null?
	}
	qp := queryprocessor.NewQueryProcessor(query.ctx)
	parentFieldsCnt := len(parentRows[0].Cols) - 2 // -2, because row is [parent_cols..., current_key, current_value]
	// in calculateSingleAvgBucket we calculate avg all current_keys with the same parent_cols
	// so we need to split into buckets based on parent_cols
	for _, parentRowsOneBucket := range qp.SplitResultSetIntoBuckets(parentRows, parentFieldsCnt) {
		resultRows = append(resultRows, query.calculateSingleAvgBucket(parentRowsOneBucket))
	}
	return resultRows
}

// we're sure len(parentRows) > 0
func (query AverageBucket) calculateSingleAvgBucket(parentRows []model.QueryResultRow) model.QueryResultRow {
	if len(parentRows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no parent rows, should NEVER happen")
		return model.QueryResultRow{}
	}

	var resultValue float64
	rowsCnt := 0
	if _, firstRowValueIsFloat := util.ExtractFloat64Maybe(parentRows[0].LastColValue()); firstRowValueIsFloat {
		sum := 0.0
		for _, parentRow := range parentRows {
			value, ok := util.ExtractFloat64Maybe(parentRow.LastColValue())
			if ok {
				sum += value
				rowsCnt++
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: %v, type: %T. Skipping", parentRow.LastColValue(), parentRow.LastColValue())
			}
		}
		resultValue = sum / float64(rowsCnt)
	} else {
		var sum int64
		for _, parentRow := range parentRows {
			value, ok := util.ExtractInt64Maybe(parentRow.LastColValue())
			if ok {
				sum += value
				rowsCnt++
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to int: %v, type: %T. Skipping", parentRow.LastColValue(), parentRow.LastColValue())
			}
		}
		resultValue = float64(sum) / float64(rowsCnt)
	}

	resultRow := parentRows[0].Copy()
	resultRow.Cols[len(resultRow.Cols)-1].Value = resultValue
	return resultRow
}

func (query AverageBucket) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}

func (query AverageBucket) String() string {
	return fmt.Sprintf("avg_bucket(%s)", query.Parent)
}
