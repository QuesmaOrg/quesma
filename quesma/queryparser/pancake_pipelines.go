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
		if pipeline.queryType.AggregationType() != model.PipelineMetricsAggregation {
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
	subAggrRows [][]model.QueryResultRow) (resultRowsPerPipeline map[string][]model.QueryResultRow) {

	resultRowsPerPipeline = make(map[string][]model.QueryResultRow)

	for _, childPipeline := range nextLayer.childrenPipelineAggregations {
		if childPipeline.queryType.AggregationType() != model.PipelineBucketAggregation {
			continue
		}

		needToAddProperMetricColumn := !childPipeline.queryType.IsCount() // If count, last column of bucketRows is already count we need.
		if needToAddProperMetricColumn {
			bucketRows = p.addProperPipelineColumn(childPipeline.parentInternalName, bucketRows, subAggrRows)
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
func (p pancakePipelinesProcessor) addProperPipelineColumn(parentColumnName string, bucketRows []model.QueryResultRow,
	subAggrRows [][]model.QueryResultRow) (newBucketRows []model.QueryResultRow) {

	if len(subAggrRows) == 0 {
		return
	}
	if len(subAggrRows[0]) == 0 {
		logger.ErrorWithCtx(p.ctx).Msg("subAggrRows[0] is empty, something is very wrong")
		return
	}

	colIdx := -1
	for i, col := range subAggrRows[0][0].Cols {
		// all subAggrRows have the same columns, so we may just look at [0][0] for simplicity
		if col.ColName == parentColumnName {
			colIdx = i
			break
		}
	}

	newBucketRows = bucketRows
	if colIdx == -1 {
		logger.WarnWithCtx(p.ctx).Msgf("could not find parent column %s", parentColumnName)
		return
	}

	for i := range bucketRows {
		// for given i, subAggrRows[i][0, 1, ...] have the same value of the metric we need, so we may just look at [0]
		newBucketRows[i].Cols[len(newBucketRows[i].Cols)-1].Value = subAggrRows[i][0].Cols[colIdx].Value
	}
	return
}
