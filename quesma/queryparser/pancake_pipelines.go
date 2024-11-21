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

	if bucketAggregation == nil {
		return rows
	}

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
		var err error
		if resultPerPipeline, err = util.Merge(resultPerPipeline, thisPipelineResults, errorMsg); err != nil {
			logger.ErrorWithCtx(p.ctx).Msgf("error merging results: %v", err)
		}
	}

	return
}

func (p pancakePipelinesProcessor) calcSingleMetricPipeline(layer *pancakeModelLayer,
	pipeline *pancakeModelPipelineAggregation, rows []model.QueryResultRow) (resultPerPipeline map[string]model.JsonMap) {

	resultPerPipeline = make(map[string]model.JsonMap)

	pipelineRows := p.selectPipelineRows(pipeline.queryType, rows, layer.nextBucketAggregation)
	resultRows := pipeline.queryType.CalculateResultWhenMissing(pipelineRows)
	resultPerPipeline[pipeline.name] = pipeline.queryType.TranslateSqlResponseToJson(resultRows)

	for _, pipelineChild := range layer.findPipelineChildren(pipeline) {
		childResults := p.calcSingleMetricPipeline(layer, pipelineChild, resultRows)

		errorMsg := fmt.Sprintf("processSingleMetricPipeline, pipeline: %s, pipelineChild: %s", pipeline.internalName, pipelineChild.internalName)
		var err error
		if resultPerPipeline, err = util.Merge(resultPerPipeline, childResults, errorMsg); err != nil {
			logger.ErrorWithCtx(p.ctx).Msgf("error merging results: %v", err)
		}
	}

	return
}

// input parameters: bucketRows is a subset of rows (it both has <= columns, and <= rows).
func (p pancakePipelinesProcessor) currentPipelineBucketAggregations(layer, nextLayer *pancakeModelLayer, bucketRows []model.QueryResultRow,
	subAggrRows [][]model.QueryResultRow) (resultRowsPerPipeline map[string][]model.JsonMap) {

	resultRowsPerPipeline = make(map[string][]model.JsonMap)

	for _, childPipeline := range nextLayer.childrenPipelineAggregations {
		if childPipeline.queryType.AggregationType() != model.PipelineBucketAggregation {
			continue
		}

		bucketRowsWithRightLastColumn := bucketRows
		needToAddProperMetricColumn := !childPipeline.queryType.IsCount() // If count, last column of bucketRows is already count we need.
		if needToAddProperMetricColumn {
			bucketRowsWithRightLastColumn = p.replaceCountColumnWithMetricColumn(childPipeline.parentInternalName, bucketRows, subAggrRows)
		}

		bucketRowsTransformedIfNeeded := bucketRowsWithRightLastColumn
		switch queryType := layer.nextBucketAggregation.queryType.(type) {
		// Current logic is not perfect, but we need extra buckets in some cases
		case *bucket_aggregations.Histogram:
			bucketRowsTransformedIfNeeded = queryType.NewRowsTransformer().Transform(p.ctx, bucketRowsWithRightLastColumn)
		case *bucket_aggregations.DateHistogram:
			bucketRowsTransformedIfNeeded = queryType.NewRowsTransformer().Transform(p.ctx, bucketRowsWithRightLastColumn)
		}

		childResults := p.calcSinglePipelineBucket(nextLayer, childPipeline, bucketRowsTransformedIfNeeded)
		for pipelineName, pipelineResults := range childResults {
			if _, alreadyExists := resultRowsPerPipeline[pipelineName]; alreadyExists { // sanity check
				logger.ErrorWithCtx(p.ctx).Msgf("pipeline %s already exists in resultsPerPipeline", pipelineName)
			}
			jsonResults := make([]model.JsonMap, len(pipelineResults))
			for i, pipelineResult := range pipelineResults {
				jsonResults[i] = childPipeline.queryType.TranslateSqlResponseToJson([]model.QueryResultRow{pipelineResult})
			}
			resultRowsPerPipeline[pipelineName] = jsonResults
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

func (p pancakePipelinesProcessor) replaceCountColumnWithMetricColumn(parentColumnName string, bucketRows []model.QueryResultRow,
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

	if colIdx == -1 {
		logger.WarnWithCtx(p.ctx).Msgf("could not find parent column %s", parentColumnName)
		return bucketRows
	}

	newBucketRows = make([]model.QueryResultRow, len(bucketRows))
	for i, origRow := range bucketRows {
		withoutLastColumn := origRow.Cols[:len(origRow.Cols)-1]

		newBucketRows[i] = model.QueryResultRow{
			Index: origRow.Index,
			Cols:  append(withoutLastColumn, subAggrRows[i][0].Cols[colIdx]),
		}
	}
	return
}
