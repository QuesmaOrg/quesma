// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package pipeline_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"strings"
)

type BucketScript struct {
	*PipelineAggregation
	script string
}

func NewBucketScript(ctx context.Context, path, script string) BucketScript {
	return BucketScript{script: script, PipelineAggregation: newPipelineAggregation(ctx, path)}
}

func (query BucketScript) AggregationType() model.AggregationType {
	return model.PipelineMetricsAggregation // not sure
}

func (query BucketScript) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	const defaultValue = 0.
	switch {
	case query.script == "params.numerator != null && params.denominator != null && params.denominator != 0 ? params.numerator / params.denominator : 0":
		parent := query.GetPathToParent()
		if len(parent) != 1 {
			// TODO: research if this limitation can be removed, and do so if possible.
			logger.WarnWithCtx(query.ctx).Msgf("unexpected parent path in bucket_script: %s. Returning default.", query.String())
			return model.JsonMap{"value": defaultValue}
		}

		// replaceAll - hack but get the job done for the customer's case, and won't break anything in any other case.
		numerator := query.findFilterValue(rows, strings.ReplaceAll(parent[0], "denominator", "numerator"))
		denominator := query.findFilterValue(rows, strings.ReplaceAll(parent[0], "numerator", "denominator"))
		if denominator == 0 {
			return model.JsonMap{"value": defaultValue}
		}
		return model.JsonMap{"value": numerator / denominator}
	case len(rows) == 1:
		for _, row := range rows {
			return model.JsonMap{"value": util.ExtractNumeric64(row.LastColValue())}
		}
	}

	logger.WarnWithCtx(query.ctx).Msgf("unexpected result in bucket_script: %s, len(rows): %d. Returning default.", query.String(), len(rows))
	return model.JsonMap{"value": defaultValue}
}

func (query BucketScript) CalculateResultWhenMissing(parentRows []model.QueryResultRow) []model.QueryResultRow {
	resultRows := make([]model.QueryResultRow, 0, len(parentRows))
	for _, parentRow := range parentRows {
		resultRow := parentRow.Copy()
		if len(resultRow.Cols) != 0 {
			resultRow.Cols[len(resultRow.Cols)-1].Value = util.ExtractNumeric64(parentRow.LastColValue())
		} else {
			logger.ErrorWithCtx(query.ctx).Msgf("unexpected empty parent row in bucket_script: %s", query.String())
		}
		resultRows = append(resultRows, resultRow)
	}
	return resultRows
}

func (query BucketScript) String() string {
	return fmt.Sprintf("bucket_script(isCount: %v, parent: %s, pathToParent: %v, parentBucketAggregation: %v, script: %v)",
		query.isCount, query.Parent, query.PathToParent, query.parentBucketAggregation, query.script)
}

func (query BucketScript) PipelineAggregationType() model.PipelineAggregationType {
	return model.PipelineParentAggregation // not sure, maybe it's sibling. change hasn't changed the result when running some tests.
}

func (query BucketScript) findFilterValue(rows []model.QueryResultRow, filterName string) float64 {
	const defaultValue = 0.0
	for _, row := range rows {
		for _, col := range row.Cols {
			colName := col.ColName
			switch { // remove possible suffix
			case strings.HasSuffix(colName, "_col_0"):
				colName = strings.TrimSuffix(colName, "_col_0")
			case strings.HasSuffix(colName, "__count"):
				colName = strings.TrimSuffix(colName, "__count")
			}
			if strings.HasSuffix(colName, filterName) {
				return util.ExtractNumeric64(col.Value)
			}
		}
	}

	logger.WarnWithCtx(query.ctx).Msgf("could not find filter value for filter: %s, bucket_script: %s, len(rows): %d."+
		"Returning default", filterName, query.String(), len(rows))
	return defaultValue
}
