// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
	"strings"
)

type BucketScript struct {
	*PipelineAggregation
	script string
}

func NewBucketScript(ctx context.Context, script string) BucketScript {
	return BucketScript{script: script, PipelineAggregation: newPipelineAggregation(ctx, "_count")}
}

func (query BucketScript) AggregationType() model.AggregationType {
	return model.PipelineMetricsAggregation // not sure
}

func (query BucketScript) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	const defaultValue = 0.
	switch query.script {
	case "params.numerator != null && params.denominator != null && params.denominator != 0 ? params.numerator / params.denominator : 0":
		numerator := query.findFilterValue("numerator", rows)
		denominator := query.findFilterValue("denominator", rows)
		if denominator == 0 {
			return model.JsonMap{"value": defaultValue}
		}
		return model.JsonMap{"value": numerator / denominator}
	default:
		for _, row := range rows {
			return model.JsonMap{"value": util.ExtractNumeric64(row.LastColValue())}
		}
	}

	return model.JsonMap{"value": 0.0}
}

func (query BucketScript) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
	//fmt.Println("bucket_script", query.String(), parentRows[:max(0, len(parentRows))])
	if len(parentRows) == 0 {
		//logger.WarnWithCtx(query.ctx).Msg("no rows returned for bucket script aggregation")
		return parentRows
	}
	resultRows := make([]model.QueryResultRow, 0, len(parentRows))
	for _, parentRow := range parentRows {
		resultRow := parentRow.Copy()
		resultRow.Cols[len(resultRow.Cols)-1].Value = util.ExtractNumeric64(parentRow.LastColValue())
		//fmt.Printf("last col %T %v", resultRow.LastColValue(), resultRow.LastColValue())
		resultRows = append(resultRows, resultRow)
	}
	return resultRows
}

func (query BucketScript) String() string {
	return fmt.Sprintf("bucket script(isCount: %v, parent: %s, pathToParent: %v, parentBucketAggregation: %v, script: %v)",
		query.isCount, query.Parent, query.PathToParent, query.parentBucketAggregation, query.script)
}

func (query BucketScript) PipelineAggregationType() model.PipelineAggregationType {
	return model.PipelineParentAggregation // not sure, maybe it's sibling. change hasn't changed the result when running some tests.
}

func (query BucketScript) findFilterValue(filterName string, rows []model.QueryResultRow) float64 {
	for _, row := range rows {
		for _, col := range row.Cols {
			colName := col.ColName
			if !strings.HasSuffix(colName, "_col_0") {
				continue
			}
			colName = strings.TrimSuffix(colName, "_col_0")
			if strings.HasSuffix(colName, "-"+filterName) {
				return float64(util.ExtractInt64(col.Value))
			}
		}
	}

	logger.WarnWithCtx(query.ctx).Msgf("could not find filter value for filter: %s", filterName)
	return 0.0
}
