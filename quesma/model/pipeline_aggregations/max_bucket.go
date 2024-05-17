package pipeline_aggregations

import (
	"context"
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryprocessor"
	"mitmproxy/quesma/util"
)

type MaxBucket struct {
	ctx    context.Context
	Parent string
	// IsCount bool
}

func NewMaxBucket(ctx context.Context, bucketsPath string) MaxBucket {
	return MaxBucket{ctx: ctx, Parent: parseBucketsPathIntoParentAggregationName(ctx, bucketsPath)}
}

func (query MaxBucket) IsBucketAggregation() bool {
	return false
}

// FIXME I think we should return all rows, not just 1
// dunno why it's working, maybe I'm wrong
func (query MaxBucket) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
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

// TODO unify with min_bucket, move to common
func (query MaxBucket) CalculateResultWhenMissing(qwa model.QueryWithAggregation, parentRows []model.QueryResultRow) []model.QueryResultRow {
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
func (query MaxBucket) calculateSingleMinBucket(parentRows []model.QueryResultRow) model.QueryResultRow {
	var resultValue any
	var resultKeys []any
	if firstRowValueFloat, firstRowValueIsFloat := util.ExtractFloat64Maybe(parentRows[0].LastColValue()); firstRowValueIsFloat {
		// find max
		maxValue := firstRowValueFloat
		for _, row := range parentRows[1:] {
			value, ok := util.ExtractFloat64Maybe(row.LastColValue())
			if ok {
				maxValue = max(maxValue, value)
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: %v, type: %T. Skipping", row.LastColValue(), row.LastColValue())
			}
		}
		resultValue = maxValue
		// find keys with max value
		for _, row := range parentRows {
			if value, ok := util.ExtractFloat64Maybe(row.LastColValue()); ok && value == maxValue {
				resultKeys = append(resultKeys, getKey(query.ctx, row))
			}
		}
	} else {
		// find max
		minValue := util.ExtractInt64(parentRows[0].LastColValue())
		for _, row := range parentRows[1:] {
			minValue = min(minValue, util.ExtractInt64(row.LastColValue()))
		}
		resultValue = minValue
		// find keys with max value
		for _, row := range parentRows {
			if value := util.ExtractInt64(row.LastColValue()); value == minValue {
				resultKeys = append(resultKeys, getKey(query.ctx, row))
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

func (query MaxBucket) String() string {
	return fmt.Sprintf("max_bucket(%s)", query.Parent)
}
