package pipeline_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryprocessor"
	"mitmproxy/quesma/util"
)

type MinBucket struct {
	ctx    context.Context
	Parent string
	// IsCount bool
}

func NewMinBucket(ctx context.Context, bucketsPath string) MinBucket {
	return MinBucket{ctx: ctx, Parent: parseBucketsPathIntoParentAggregationName(ctx, bucketsPath)}
}

func (query MinBucket) IsBucketAggregation() bool {
	return false
}

func (query MinBucket) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for average bucket aggregation")
		return []model.JsonMap{nil}
	}
	if len(rows) > 1 {
		logger.WarnWithCtx(query.ctx).Msg("more than one row returned for average bucket aggregation")
	}
	if returnMap, ok := rows[0].LastColValue().(model.JsonMap); ok {
		return []model.JsonMap{returnMap}
	} else {
		logger.WarnWithCtx(query.ctx).Msgf("could not convert value to JsonMap: %v, type: %T", rows[0].LastColValue(), rows[0].LastColValue())
		return []model.JsonMap{nil}
	}
}

func (query MinBucket) CalculateResultWhenMissing(qwa *model.Query, parentRows []model.QueryResultRow) []model.QueryResultRow {
	fmt.Println("hoho")
	fmt.Println(parentRows)
	resultRows := make([]model.QueryResultRow, 0)
	if len(parentRows) == 0 {
		return resultRows // maybe null?
	}
	qp := queryprocessor.NewQueryProcessor(query.ctx)
	for _, parentRowsOneBucket := range qp.SplitResultSetIntoBuckets(parentRows, len(parentRows[0].Cols)-3) {
		resultRows = append(resultRows, query.calculateSingleMinBucket(parentRowsOneBucket))
	}
	fmt.Println("resultRows", resultRows)
	return resultRows
}

// we're sure len(parentRows) > 0
func (query MinBucket) calculateSingleMinBucket(parentRows []model.QueryResultRow) model.QueryResultRow {
	var resultValue any
	var resultKeys []any
	if firstRowValueFloat, firstRowValueIsFloat := util.ExtractFloat64Maybe(parentRows[0].LastColValue()); firstRowValueIsFloat {
		// find min
		minValue := firstRowValueFloat
		for _, row := range parentRows[1:] {
			value, ok := util.ExtractFloat64Maybe(row.LastColValue())
			if ok {
				minValue = min(minValue, value)
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: %v, type: %T. Skipping", row.LastColValue(), row.LastColValue())
			}
		}
		resultValue = minValue
		// find keys with min value
		for _, row := range parentRows {
			if value, ok := util.ExtractFloat64Maybe(row.LastColValue()); ok && value == minValue {
				resultKeys = append(resultKeys, query.getKey(row))
			}
		}
	} else {
		// find min
		minValue := util.ExtractInt64(parentRows[0].LastColValue())
		for _, row := range parentRows[1:] {
			minValue = min(minValue, util.ExtractInt64(row.LastColValue()))
		}
		resultValue = minValue
		// find keys with min value
		for _, row := range parentRows {
			if value := util.ExtractInt64(row.LastColValue()); value == minValue {
				resultKeys = append(resultKeys, query.getKey(row))
			}
		}
	}

	resultRow := parentRows[0].Copy()
	resultRow.Cols[len(resultRow.Cols)-1].Value = model.JsonMap{
		"value": resultValue,
		"keys":  resultKeys,
	}
	return resultRow
}

func (query MinBucket) getKey(row model.QueryResultRow) any {
	if len(row.Cols) < 2 {
		logger.WarnWithCtx(query.ctx).Msgf("row has less than 2 columns: %v", row)
		return nil
	}
	return row.Cols[len(row.Cols)-2].Value
}

func (query MinBucket) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}

func (query MinBucket) String() string {
	return fmt.Sprintf("min_bucket(%s)", query.Parent)
}
