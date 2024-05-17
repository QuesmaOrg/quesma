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
	isCount := bucketsPath == BucketsPathCount
	return CumulativeSum{ctx: ctx, Parent: bucketsPath, IsCount: isCount}
}

const BucketsPathCount = "_count" // special name for `buckets_path` parameter, normally it's some other aggregation's name

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

func (query CumulativeSum) CalculateResultWhenMissing(qwa *model.Query, parentRows []model.QueryResultRow) []model.QueryResultRow {
	resultRows := make([]model.QueryResultRow, 0, len(parentRows))
	if len(parentRows) == 0 {
		return resultRows
	}

	if _, firstRowValueIsFloat := util.ExtractFloat64Maybe(parentRows[0].LastColValue()); firstRowValueIsFloat {
		sum := 0.0
		for _, parentRow := range parentRows {
			value, ok := util.ExtractFloat64Maybe(parentRow.LastColValue())
			if ok {
				sum += value
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to float: %v, type: %T. Skipping", parentRow.LastColValue(), parentRow.LastColValue())
			}
			resultRow := parentRow.Copy()
			resultRow.Cols[len(resultRow.Cols)-1].Value = sum
			resultRows = append(resultRows, resultRow)
		}
	} else { // cumulative sum must be on numeric, so if it's not float64, it should always be int
		var sum int64
		for _, parentRow := range parentRows {
			value, ok := util.ExtractInt64Maybe(parentRow.LastColValue())
			if ok {
				sum += value
			} else {
				logger.WarnWithCtx(query.ctx).Msgf("could not convert value to int: %v, type: %T. Skipping", parentRow.LastColValue(), parentRow.LastColValue())
			}
			resultRow := parentRow.Copy()
			resultRow.Cols[len(resultRow.Cols)-1].Value = sum
			resultRows = append(resultRows, resultRow)
		}
	}
	return resultRows
}

func (query CumulativeSum) PostprocessResults(rowsFromDB []model.QueryResultRow) []model.QueryResultRow {
	return rowsFromDB
}

func (query CumulativeSum) String() string {
	return fmt.Sprintf("cumulative_sum(%s)", query.Parent)
}
