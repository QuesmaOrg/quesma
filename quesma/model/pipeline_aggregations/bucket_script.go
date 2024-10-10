// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/util"
)

type BucketScript struct {
	*PipelineAggregation
}

func NewBucketScript(ctx context.Context) BucketScript {
	return BucketScript{PipelineAggregation: newPipelineAggregation(ctx, "_count")}
}

func (query BucketScript) AggregationType() model.AggregationType {
	return model.PipelineBucketAggregation // not sure
}

func (query BucketScript) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	fmt.Println("bucket_script", rows)
	if len(rows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for bucket script aggregation")
	}
	for _, row := range rows {
		return model.JsonMap{"value": int64(util.ExtractInt64(row.LastColValue()))}
	}
	return model.JsonMap{"value": 0.}
}

func (query BucketScript) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
	fmt.Println("bucket_script", parentRows)
	if len(parentRows) == 0 {
		logger.WarnWithCtx(query.ctx).Msg("no rows returned for bucket script aggregation")
		return parentRows
	}
	resultRows := make([]model.QueryResultRow, 0, len(parentRows))
	for _, parentRow := range parentRows {
		resultRow := parentRow.Copy()
		resultRow.Cols[len(resultRow.Cols)-1].Value = float64(util.ExtractInt64(parentRow.LastColValue()))
		fmt.Printf("last col %T %v", resultRow.LastColValue(), resultRow.LastColValue())
		resultRows = append(resultRows, resultRow)
	}
	return resultRows
}

func (query BucketScript) String() string {
	return fmt.Sprintf("bucket script(isCount: %v, parent: %s, pathToParent: %v, parentBucketAggregation: %v)",
		query.isCount, query.Parent, query.PathToParent, query.parentBucketAggregation)
}

func (query BucketScript) PipelineAggregationType() model.PipelineAggregationType {
	return model.PipelineParentAggregation // not sure, maybe it's sibling.
}
