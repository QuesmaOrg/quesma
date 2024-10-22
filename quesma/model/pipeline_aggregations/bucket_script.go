// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"quesma/model"
	"quesma/util"
	"strings"
)

type BucketScript struct {
	script string
	*PipelineAggregation
}

func NewBucketScript(ctx context.Context, script string) BucketScript {
	return BucketScript{script: script, PipelineAggregation: newPipelineAggregation(ctx, "_count")}
}

func (query BucketScript) AggregationType() model.AggregationType {
	return model.PipelineMetricsAggregation // not sure
}

func (query BucketScript) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	//fmt.Println("bucket_script", query.String(), rows)
	if len(rows) == 0 {
		//logger.WarnWithCtx(query.ctx).Msg("no rows returned for bucket script aggregation")
	}
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
			return model.JsonMap{"value": util.ExtractInt64(row.LastColValue())}
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
		resultRow.Cols[len(resultRow.Cols)-1].Value = float64(util.ExtractInt64(parentRow.LastColValue()))
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
	return model.PipelineParentAggregation // not sure, maybe it's sibling. doesnt change the result
}

func (query BucketScript) findFilterValue(filterName string, rows []model.QueryResultRow) float64 {
	for _, row := range rows {
		for _, col := range row.Cols {
			//fmt.Println("col", col)
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
	return 0
}
