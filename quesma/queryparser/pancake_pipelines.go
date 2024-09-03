// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/util"
)

type pancakePipelinesProcessor struct {
	ctx context.Context
}

func (p pancakePipelinesProcessor) selectPipelineRows(pipeline model.PipelineQueryType, rows []model.QueryResultRow,
	bucketAggregation *pancakeModelBucketAggregation) (
	result []model.QueryResultRow) {

	isCount := pipeline.IsCount()
	for _, row := range rows {
		newRow := model.QueryResultRow{Index: row.Index}
		for _, col := range row.Cols {
			if !isCount && bucketAggregation.isInternalNameCountColumn(col.ColName) {
				continue
			}
			if !bucketAggregation.isInternalNameOrderByColumn(col.ColName) {
				// we don't need order by (and actually it would break if we included them)
				newRow.Cols = append(newRow.Cols, col)
			}
		}
		fmt.Println()
		result = append(result, newRow)
	}
	return
}

func (p pancakePipelinesProcessor) currentPipelineMetricAggregations(layer *pancakeModelLayer,
	rows []model.QueryResultRow) (resultPerPipeline map[string]model.JsonMap) {

	resultPerPipeline = make(map[string]model.JsonMap)

	for _, pipeline := range layer.childrenPipelineAggregations {
		typ := pipeline.queryType.PipelineAggregationType()
		if typ != model.PipelineMetricsAggregation {
			continue
		}

		thisPipelineResults := p.calcSingleMetricPipeline(layer, pipeline, rows)

		errorMsg := fmt.Sprintf("calculateThisLayerMetricPipelines, pipeline: %s", pipeline.internalName)
		resultPerPipeline = util.Merge(p.ctx, resultPerPipeline, thisPipelineResults, errorMsg)
	}

	return
}

func (p pancakePipelinesProcessor) calcSingleMetricPipeline(layer *pancakeModelLayer,
	pipeline *pancakeModelPipelineAggregation, rows []model.QueryResultRow) (resultPerPipeline map[string]model.JsonMap) {

	resultPerPipeline = make(map[string]model.JsonMap)

	pipelineRows := p.selectPipelineRows(pipeline.queryType, rows, layer.nextBucketAggregation)
	resultRows := pipeline.queryType.CalculateResultWhenMissing(pipelineRows)
	resultPerPipeline[pipeline.name] = pipeline.queryType.TranslateSqlResponseToJson(resultRows, 0) // TODO: fill level?

	for _, pipelineChild := range layer.findPipelineChildren(pipeline) {
		childResults := p.calcSingleMetricPipeline(layer, pipelineChild, resultRows)

		errorMsg := fmt.Sprintf("processSingleMetricPipeline, pipeline: %s, pipelineChild: %s", pipeline.internalName, pipelineChild.internalName)
		resultPerPipeline = util.Merge(p.ctx, resultPerPipeline, childResults, errorMsg)
	}

	return
}

// input parameters: bucketRows is a subset of rows (it both has <= columns, and <= rows).
// If e.g. rowIndexes = [2, 5], then bucketRows = [rows[2], rows[5]] (with maybe some columns removed)
// We need rows and rowIndexes to fetch proper metric column from rows.
func (p pancakePipelinesProcessor) currentPipelineBucketAggregations(layer, nextLayer *pancakeModelLayer, bucketRows []model.QueryResultRow,
	rows []model.QueryResultRow, rowIndexes []int) (resultRowsPerPipeline map[string][]model.QueryResultRow) {

	resultRowsPerPipeline = make(map[string][]model.QueryResultRow)

	for _, childPipeline := range nextLayer.childrenPipelineAggregations {
		if childPipeline.queryType.PipelineAggregationType() != model.PipelineBucketAggregation {
			continue
		}

		var oldColumnArr []any
		needToAddProperMetricColumn := !childPipeline.queryType.IsCount() // If count, last column of bucketRows is already count we need.
		if needToAddProperMetricColumn {
			bucketRows, oldColumnArr = p.addProperPipelineColumn(childPipeline.parentInternalName, bucketRows, rows, rowIndexes)
		}

		var bucketRowsTransformedIfNeeded []model.QueryResultRow
		switch queryType := layer.nextBucketAggregation.queryType.(type) {
		// TODO: logic what and when to transform shouldn't probably be here
		case bucket_aggregations.Histogram:
			bucketRowsTransformedIfNeeded = queryType.NewRowsTransformer().Transform(p.ctx, bucketRows)
		case *bucket_aggregations.DateHistogram:
			bucketRowsTransformedIfNeeded = queryType.NewRowsTransformer().Transform(p.ctx, bucketRows)
		default:
			bucketRowsTransformedIfNeeded = bucketRows
		}

		childResults := p.calcSinglePipelineBucket(nextLayer, childPipeline, bucketRowsTransformedIfNeeded)
		for pipelineName, pipelineResults := range childResults {
			if _, alreadyExists := resultRowsPerPipeline[pipelineName]; alreadyExists { // sanity check
				logger.ErrorWithCtx(p.ctx).Msgf("pipeline %s already exists in resultsPerPipeline", pipelineName)
			}
			resultRowsPerPipeline[pipelineName] = pipelineResults
		}

		if needToAddProperMetricColumn {
			bucketRows = p.restoreOriginalColumn(bucketRows, oldColumnArr)
		}
	}

	return
}

func (p pancakePipelinesProcessor) calcSinglePipelineBucket(layer *pancakeModelLayer, pipeline *pancakeModelPipelineAggregation,
	bucketRows []model.QueryResultRow) (resultRowsPerPipeline map[string][]model.QueryResultRow) {

	resultRowsPerPipeline = make(map[string][]model.QueryResultRow)

	currentPipelineResults := pipeline.queryType.CalculateResultWhenMissing(bucketRows)
	resultRowsPerPipeline[pipeline.name] = currentPipelineResults

	for _, pipelineChild := range layer.findPipelineChildren(pipeline) {
		childPipelineResults := p.calcSinglePipelineBucket(layer, pipelineChild, currentPipelineResults)
		for name, results := range childPipelineResults {
			if _, alreadyExists := resultRowsPerPipeline[name]; alreadyExists { // sanity check
				logger.ErrorWithCtx(p.ctx).Msgf("pipeline %s already exists in resultsPerPipeline", name)
			}
			resultRowsPerPipeline[name] = results
		}
	}

	return
}

// returns:
//   - newSelectedRows: same as selectedRows, but with one column different if needed (value for this column is taken from
//     allRows, which has >= columns than selectedRows, and should have the column we need)
//   - oldColumnArray:  old value of the exchanged column, to be restored in restoreOriginalColumn after processing
//
// Use restoreOriginalColumn after processing to restore original values.
func (p pancakePipelinesProcessor) addProperPipelineColumn(parentColumnName string, selectedRows, allRows []model.QueryResultRow,
	selectedRowsIndexes []int) (newSelectedRows []model.QueryResultRow, oldColumnArray []any) {

	if len(allRows) == 0 {
		return
	}

	colIdx := -1
	for i, col := range allRows[0].Cols {
		if col.ColName == parentColumnName {
			colIdx = i
			break
		}
	}

	oldColumnArray = make([]any, 0, len(selectedRows))
	newSelectedRows = selectedRows
	if colIdx == -1 {
		logger.WarnWithCtx(p.ctx).Msgf("could not find parent column %s", parentColumnName)
		for _, row := range selectedRows {
			oldColumnArray = append(oldColumnArray, row.LastColValue())
		}
	} else {
		for i := range selectedRows {
			oldColumnArray = append(oldColumnArray, selectedRows[i].LastColValue())
			newSelectedRows[i].Cols[len(newSelectedRows[i].Cols)-1].Value = allRows[selectedRowsIndexes[i]].Cols[colIdx].Value
		}
	}

	return
}

// used after addProperPipelineColumn
func (p pancakePipelinesProcessor) restoreOriginalColumn(rows []model.QueryResultRow, valuesToRestore []any) []model.QueryResultRow {
	for i, row := range rows {
		row.Cols[len(row.Cols)-1].Value = valuesToRestore[i]
	}
	return rows
}
