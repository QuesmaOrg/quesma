package pipeline_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryprocessor"
	"mitmproxy/quesma/util"
)

type SumBucket struct {
	ctx    context.Context
	Parent string
}

func NewSumBucket(ctx context.Context, bucketsPath string) SumBucket {
	return SumBucket{ctx: ctx, Parent: parseBucketsPathIntoParentAggregationName(ctx, bucketsPath)}
}

func (query SumBucket) IsBucketAggregation() bool {
	return false
}

func (query SumBucket) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for average bucket aggregation")
		return []model.JsonMap{nil}
	}
	if len(rows) > 1 {
		logger.WarnWithCtx(query.ctx).Msg("more than one row returned for average bucket aggregation")
	}
	if returnMap, ok := rows[0].LastColValue().(model.JsonMap); ok {
		return []model.JsonMap{returnMap}
	}
	logger.WarnWithCtx(query.ctx).Msgf("could not convert value to JsonMap: %v, type: %T", rows[0].LastColValue(), rows[0].LastColValue())
	return []model.JsonMap{nil}
}

func (query SumBucket) CalculateResultWhenMissing(qwa *model.Query, parentRows []model.QueryResultRow) []model.QueryResultRow {
	resultRows := make([]model.QueryResultRow, 0)
	if len(parentRows) == 0 {
		return resultRows // maybe null?
	}
	qp := queryprocessor.NewQueryProcessor(query.ctx)
	parentFieldsCnt := len(parentRows[0].Cols) - 2 // -2, because row is [parent_cols..., current_key, current_value]
	// in calculateSingleAvgBucket we calculate avg all current_keys with the same parent_cols
	// so we need to split into buckets based on parent_cols
	if parentFieldsCnt < 0 {
		logger.WarnWithCtx(query.ctx).Msgf("parentFieldsCnt is less than 0: %d", parentFieldsCnt)
	}
	for _, parentRowsOneBucket := range qp.SplitResultSetIntoBuckets(parentRows, parentFieldsCnt) {
		resultRows = append(resultRows, query.calculateSingleSumBucket(parentRowsOneBucket))
	}
	return resultRows
}

// we're sure len(parentRows) > 0
func (query SumBucket) calculateSingleSumBucket(parentRows []model.QueryResultRow) model.QueryResultRow {
	var resultValue any
	if firstRowValueFloat, firstRowValueIsFloat := util.ExtractFloat64Maybe(parentRows[0].LastColValue()); firstRowValueIsFloat {
		sum := firstRowValueFloat
		for _, row := range parentRows[1:] {
			value, ok := util.ExtractFloat64Maybe(row.LastColValue())
			if ok {
				sum += value
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: %v, type: %T. Skipping", row.LastColValue(), row.LastColValue())
			}
		}
		resultValue = sum
	} else if firstRowValueInt, firstRowValueIsInt := util.ExtractInt64Maybe(parentRows[0].LastColValue()); firstRowValueIsInt {
		sum := firstRowValueInt
		for _, row := range parentRows[1:] {
			value, ok := util.ExtractInt64Maybe(row.LastColValue())
			if ok {
				sum += value
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to int: %v, type: %T. Skipping", row.LastColValue(), row.LastColValue())
			}
		}
		resultValue = sum
	}

	resultRow := parentRows[0].Copy()
	resultRow.Cols[len(resultRow.Cols)-1].Value = model.JsonMap{
		"value": resultValue,
	}
	return resultRow
}

func (query SumBucket) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}

func (query SumBucket) String() string {
	return fmt.Sprintf("sum_bucket(%s)", query.Parent)
}
