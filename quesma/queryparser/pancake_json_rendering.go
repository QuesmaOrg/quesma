// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/metrics_aggregations"
	"quesma/util"
	"strconv"
	"strings"
)

type pancakeJSONRenderer struct {
	ctx      context.Context
	pipeline pancakePipelinesProcessor
}

func newPancakeJSONRenderer(ctx context.Context) *pancakeJSONRenderer {
	return &pancakeJSONRenderer{
		ctx:      ctx,
		pipeline: pancakePipelinesProcessor{ctx: ctx},
	}
}

func (p *pancakeJSONRenderer) selectMetricRows(metricName string, rows []model.QueryResultRow) (result []model.QueryResultRow) {
	if len(rows) > 0 {
		newRow := model.QueryResultRow{Index: rows[0].Index}
		for _, col := range rows[0].Cols {
			if strings.HasPrefix(col.ColName, metricName) {
				newRow.Cols = append(newRow.Cols, col)
			}
		}
		return []model.QueryResultRow{newRow}
	}
	logger.ErrorWithCtx(p.ctx).Msgf("no rows in selectMetricRows %s", metricName)
	return
}

// selectTopHitsRows: select columns for top_hits/top_metrics and rename them to original column names.
// There is refactoring opportunity once we move completely to pancakes and remove re-name logic from this method.
func (p *pancakeJSONRenderer) selectTopHitsRows(topAggr *pancakeModelMetricAggregation, rows []model.QueryResultRow) (result []model.QueryResultRow) {
	for _, row := range rows {
		var newCols []model.QueryResultCol
		for _, col := range row.Cols {
			if strings.HasPrefix(col.ColName, topAggr.InternalNamePrefix()) {
				numStr := strings.TrimPrefix(col.ColName, topAggr.InternalNamePrefix())
				if num, err := strconv.Atoi(numStr); err == nil {
					var overrideName string
					if num < 0 || num >= len(topAggr.selectedColumns) {
						logger.WarnWithCtx(p.ctx).Msgf("invalid top_hits column index %d", num)
					} else {
						selectedColumn := topAggr.selectedColumns[num]
						if colRef, ok := selectedColumn.(model.ColumnRef); ok {
							overrideName = colRef.ColumnName
						}
					}
					if len(overrideName) > 0 {
						col.ColName = overrideName
					}
					newCols = append(newCols, col)
				}
			}
		}
		result = append(result, model.QueryResultRow{Index: row.Index, Cols: newCols})
	}
	return
}

func (p *pancakeJSONRenderer) selectPrefixRows(prefix string, rows []model.QueryResultRow) (result []model.QueryResultRow) {
	if prefix == "" {
		return rows
	}
	for _, row := range rows {
		var newCols []model.QueryResultCol
		for _, col := range row.Cols {
			if strings.HasPrefix(col.ColName, prefix) {
				newCols = append(newCols, model.NewQueryResultCol(strings.TrimPrefix(col.ColName, prefix), col.Value))
			}
		}
		result = append(result, model.QueryResultRow{Index: row.Index, Cols: newCols})
	}
	return
}

// rowIndexes - which row in the original result set corresponds to the first row of the bucket
// It's needed for pipeline aggregations, as we might need to take some other columns from the original row to calculate them.
func (p *pancakeJSONRenderer) splitBucketRows(bucket *pancakeModelBucketAggregation, rows []model.QueryResultRow) (
	buckets []model.QueryResultRow, subAggrs [][]model.QueryResultRow) {

	if len(rows) == 0 {
		return buckets, subAggrs
	}
	bucketKeyName := bucket.InternalNameForKeyPrefix()
	bucketCountName := bucket.InternalNameForCount()
	bucketParentCountName := bucket.InternalNameForParentCount()
	indexName := rows[0].Index
	for rowIdx, row := range rows {
		isNewBucket := rowIdx == 0 // first row is always new bucket
		if !isNewBucket {          // for subsequent rows, create new bucket if any key is different
			previousBucket := buckets[len(buckets)-1]
			for _, cols := range row.Cols {
				if strings.HasPrefix(cols.ColName, bucketKeyName) {
					for _, previousCols := range previousBucket.Cols {
						if cols.ColName == previousCols.ColName {
							if cols.Value != previousCols.Value {
								isNewBucket = true
							}
							break
						}
					}
				}
			}
		}

		if isNewBucket {
			buckets = append(buckets, model.QueryResultRow{Index: indexName})
			subAggrs = append(subAggrs, []model.QueryResultRow{})
			lastIdx := len(buckets) - 1
			for _, cols := range row.Cols {
				if strings.HasPrefix(cols.ColName, bucketKeyName) || strings.HasPrefix(cols.ColName, bucketCountName) ||
					strings.HasPrefix(cols.ColName, bucketParentCountName) {
					buckets[lastIdx].Cols = append(buckets[lastIdx].Cols, cols)
				}
			}
		}
		lastIdx := len(buckets) - 1
		subAggrs[lastIdx] = append(subAggrs[lastIdx], row)
	}

	return buckets, subAggrs
}

// In some queries we want to filter out null values or empty
// We accomplish that by increasing limit by one during SQL query and then filtering out during JSON rendering.
// So we either filter out empty or last one if there is none.
// This can't be replaced by WHERE in generic case.
//
// rowIndexes - which row in the original result set corresponds to the first row of the bucket
// It's needed for pipeline aggregations, as we might need to take some other columns from the original row to calculate them.
func (p *pancakeJSONRenderer) potentiallyRemoveExtraBucket(layer *pancakeModelLayer, bucketRows []model.QueryResultRow,
	subAggrRows [][]model.QueryResultRow) ([]model.QueryResultRow, [][]model.QueryResultRow) {
	// We are filter out null
	if layer.nextBucketAggregation.filterOurEmptyKeyBucket {
		nullRowToDelete := -1
		bucketKeyName := layer.nextBucketAggregation.InternalNameForKeyPrefix()
	ROW:
		for i, row := range bucketRows {
			for _, col := range row.Cols {
				if strings.HasPrefix(col.ColName, bucketKeyName) {
					if col.Value == nil || col.Value == "" { // TODO: replace with schema
						nullRowToDelete = i
						break ROW
					}
				}
			}
		}

		if nullRowToDelete != -1 {
			bucketRows = append(bucketRows[:nullRowToDelete], bucketRows[nullRowToDelete+1:]...)
			subAggrRows = append(subAggrRows[:nullRowToDelete], subAggrRows[nullRowToDelete+1:]...)
		} else if layer.nextBucketAggregation.limit != 0 && len(bucketRows) > layer.nextBucketAggregation.limit {
			bucketRows = bucketRows[:layer.nextBucketAggregation.limit]
			subAggrRows = subAggrRows[:layer.nextBucketAggregation.limit]
		}
	}
	return bucketRows, subAggrRows
}

func (p *pancakeJSONRenderer) combinatorBucketToJSON(remainingLayers []*pancakeModelLayer, rows []model.QueryResultRow) (model.JsonMap, error) {
	layer := remainingLayers[0]
	switch queryType := layer.nextBucketAggregation.queryType.(type) {
	case bucket_aggregations.SamplerInterface, bucket_aggregations.FilterAgg:
		selectedRows := p.selectMetricRows(layer.nextBucketAggregation.InternalNameForCount(), rows)
		fmt.Println("RENDER 187", rows, selectedRows)
		aggJson := layer.nextBucketAggregation.queryType.TranslateSqlResponseToJson(selectedRows)
		subAggr, err := p.layerToJSON(remainingLayers[1:], rows)
		if err != nil {
			return nil, err
		}
		return util.MergeMaps(p.ctx, aggJson, subAggr), nil
	case bucket_aggregations.CombinatorAggregationInterface:
		var bucketArray []model.JsonMap
		for _, subGroup := range queryType.CombinatorGroups() {
			selectedRowsWithoutPrefix := p.selectPrefixRows(subGroup.Prefix, rows)
			fmt.Println("RENDER 188", rows, selectedRowsWithoutPrefix)

			subAggr, err := p.layerToJSON(remainingLayers[1:], selectedRowsWithoutPrefix)
			if err != nil {
				return nil, err
			}

			selectedRows := p.selectMetricRows(layer.nextBucketAggregation.InternalNameForCount(), selectedRowsWithoutPrefix)
			fmt.Println("RENDER 189", selectedRowsWithoutPrefix, selectedRows)
			aggJson := queryType.CombinatorTranslateSqlResponseToJson(subGroup, selectedRows)

			bucketArray = append(bucketArray, util.MergeMaps(p.ctx, aggJson, subAggr))
			bucketArray[len(bucketArray)-1]["key"] = subGroup.Key
		}
		var bucketsJson any
		if !layer.nextBucketAggregation.isKeyed {
			bucketsJson = bucketArray
		} else {
			buckets := model.JsonMap{}
			for _, bucket := range bucketArray {
				if key, ok := bucket["key"]; ok {
					delete(bucket, "key")
					buckets[key.(string)] = bucket
				} else {
					return nil, fmt.Errorf("no key in bucket json, layer: %s", layer.nextBucketAggregation.name)
				}
			}
			bucketsJson = buckets
		}
		return model.JsonMap{
			"buckets": bucketsJson,
		}, nil
	default:
		return nil, fmt.Errorf("unexpected bucket aggregation type: %T", layer.nextBucketAggregation.queryType)
	}
}

func (p *pancakeJSONRenderer) layerToJSON(remainingLayers []*pancakeModelLayer, rows []model.QueryResultRow) (model.JsonMap, error) {
	result := model.JsonMap{}
	if len(remainingLayers) == 0 {
		return result, nil
	}

	layer := remainingLayers[0]

	for _, metric := range layer.currentMetricAggregations {
		var metricRows []model.QueryResultRow
		switch metric.queryType.(type) {
		case *metrics_aggregations.TopMetrics, *metrics_aggregations.TopHits:
			metricRows = p.selectTopHitsRows(metric, rows)
		default:
			metricRows = p.selectMetricRows(metric.InternalNamePrefix(), rows)
		}
		if metric.name != PancakeTotalCountMetricName {
			//fmt.Println("RENDER 241", metricRows)
			result[metric.name] = metric.queryType.TranslateSqlResponseToJson(metricRows)
		}
		//fmt.Println("AAAAA", metric.name)
		// TODO: maybe add metadata also here? probably not needed
	}

	//fmt.Println("robie result, layer:", layer)

	// pipeline aggregations of metric type behave just like metric
	for metricPipelineAggrName, metricPipelineAggrResult := range p.pipeline.currentPipelineMetricAggregations(layer, rows) {
		//fmt.Println("metricPipelineAggrName", metricPipelineAggrName)
		result[metricPipelineAggrName] = metricPipelineAggrResult
		//fmt.Printf("\n\nwazny print\nmeta:%v\n", a)
		//TODO: maybe add metadata also here? probably not needed
	}

	if layer.nextBucketAggregation != nil {
		// sampler and filter are special
		//fmt.Println("RENDER 257", layer.nextBucketAggregation)
		if !layer.nextBucketAggregation.DoesHaveGroupBy() {
			//fmt.Println("RENDER 258 not have group by")
			json, err := p.combinatorBucketToJSON(remainingLayers, rows)
			if err != nil {
				return nil, err
			}
			if layer.nextBucketAggregation.metadata != nil {
				json["meta"] = layer.nextBucketAggregation.metadata
			}
			result[layer.nextBucketAggregation.name] = json

			return result, nil
		}

		hasSubaggregations := len(remainingLayers) > 1
		var nextLayer *pancakeModelLayer
		if hasSubaggregations {
			// If we have pipeline parent aggregation, we need to *always* set min_doc_count to 0 in the parent bucket aggregation
			// Important to do that early, before processing it after this if.
			nextLayer = remainingLayers[1]
			anyPipelineParentAggregation := false
			for _, pipeline := range nextLayer.childrenPipelineAggregations {
				if pipeline.queryType.PipelineAggregationType() == model.PipelineParentAggregation {
					anyPipelineParentAggregation = true
					break
				}
			}
			if anyPipelineParentAggregation {
				switch parentBucketAggreagation := layer.nextBucketAggregation.queryType.(type) {
				case *bucket_aggregations.DateHistogram:
					parentBucketAggreagation.SetMinDocCountToZero()
				case *bucket_aggregations.Histogram:
					parentBucketAggreagation.SetMinDocCountToZero()
				}
			}
		}

		bucketRows, subAggrRows := p.splitBucketRows(layer.nextBucketAggregation, rows)
		//fmt.Println("1 len", len(bucketRows), len(subAggrRows))
		bucketRows, subAggrRows = p.potentiallyRemoveExtraBucket(layer, bucketRows, subAggrRows)
		//fmt.Println("2 len", len(bucketRows), len(subAggrRows))
		buckets := layer.nextBucketAggregation.queryType.TranslateSqlResponseToJson(bucketRows)
		//fmt.Println("3 len", len(bucketRows), len(subAggrRows))
		//fmt.Println("RENDER 294", layer.nextBucketAggregation.queryType, "rows:", rows[:min(2, len(rows))], "bucketRows:", bucketRows[:min(2, len(bucketRows))], "subAggrRows:", subAggrRows[:min(2, len(subAggrRows))])

		if len(buckets) == 0 { // without this we'd generate {"buckets": []} in the response, which Elastic doesn't do.
			if layer.nextBucketAggregation.metadata != nil {
				buckets["meta"] = layer.nextBucketAggregation.metadata
				result[layer.nextBucketAggregation.name] = buckets
			}
			return result, nil
		}

		if hasSubaggregations {
			pipelineBucketsPerAggregation := p.pipeline.currentPipelineBucketAggregations(layer, nextLayer, bucketRows, subAggrRows)

			// Add subAggregations (both normal and pipeline)
			bucketArrRaw, ok := buckets["buckets"]
			if !ok {
				return nil, fmt.Errorf("no buckets key in bucket json, layer: %s", layer.nextBucketAggregation.name)
			}

			bucketArr := bucketArrRaw.([]model.JsonMap)

			//fmt.Println("len(bucketArr)", len(bucketArr), "len(subAggrRows)", len(subAggrRows))
			//f//mt.Println("subAggrRows", subAggrRows)
			//fm//t.Println("bucketArr", bucketArr)
			if len(bucketArr) == len(subAggrRows) {
				// Simple case, we merge bucketArr[i] with subAggrRows[i] (if lengths are equal, keys must be equal => it's fine to not check them at all)
				for i, bucket := range bucketArr {
					for pipelineAggrName, pipelineAggrResult := range pipelineBucketsPerAggregation {
						//fmt.Println("pipelineAggrName", pipelineAggrName, pipelineAggrResult)
						bucketArr[i][pipelineAggrName] = pipelineAggrResult[i]
					}

					if docCount, ok := bucket["doc_count"]; ok && fmt.Sprintf("%v", docCount) == "0" {
						// Not sure, but it does the trick.
						continue
					}

					subAggr, err := p.layerToJSON(remainingLayers[1:], subAggrRows[i])
					if err != nil {
						return nil, err
					}
					bucketArr[i] = util.MergeMaps(p.ctx, bucket, subAggr)
				}
			} else {
				// A bit harder case. Observation: len(bucketArr) > len(subAggrRows) and set(subAggrRows' keys) is a subset of set(bucketArr's keys)
				// So if bucket[i]'s key corresponds to subAggr[subAggrIdx]'s key, we merge them.
				// If not, we just keep bucket[i] (i++, subAggrIdx stays the same)
				subAggrIdx := 0
				for i, bucket := range bucketArr {
					for pipelineAggrName, pipelineAggrResult := range pipelineBucketsPerAggregation {
						bucketArr[i][pipelineAggrName] = pipelineAggrResult[i]
					}

					if docCount, ok := bucket["doc_count"]; ok && fmt.Sprintf("%v", docCount) == "0" {
						//fmt.Println("wtf? continue?")
						// Not sure, but it does the trick.
						//continue
					}

					// if our bucket aggregation is a date_histogram, we need original key, not processed one, which is "key"
					key, exists := bucket[bucket_aggregations.OriginalKeyName]
					if !exists {
						key, exists = bucket["key"]
						if !exists {
							return nil, fmt.Errorf("no key in bucket json, layer: %s", layer.nextBucketAggregation.name)
						}
					}

					columnNameWithKey := layer.nextBucketAggregation.InternalNameForKey(0) // TODO: need all ids, multi_terms will probably not work now
					found := false
					var subAggrKey any
					if len(subAggrRows) > subAggrIdx {
						subAggrKey, found = p.valueForColumn(subAggrRows[subAggrIdx], columnNameWithKey)
					}
					if found && subAggrKey == key {
						subAggr, err := p.layerToJSON(remainingLayers[1:], subAggrRows[subAggrIdx])
						if err != nil {
							return nil, err
						}
						bucketArr[i] = util.MergeMaps(p.ctx, bucket, subAggr)
						if _, exists = bucketArr[i][bucket_aggregations.OriginalKeyName]; exists {
							delete(bucketArr[i], bucket_aggregations.OriginalKeyName)
						}
						subAggrIdx++
					} else {
						x, err := p.layerToJSON(remainingLayers[1:], []model.QueryResultRow{})
						//fmt.Println("x", x, err)
						if err != nil {
							return nil, err
						}
						bucketArr[i] = util.MergeMaps(p.ctx, bucket, x)
						if _, exists = bucketArr[i][bucket_aggregations.OriginalKeyName]; exists {
							delete(bucketArr[i], bucket_aggregations.OriginalKeyName)
						}
					}
				}
			}
		}

		fmt.Println("ADDING METADATA", layer.nextBucketAggregation.metadata)
		if layer.nextBucketAggregation.metadata != nil {
			buckets["meta"] = layer.nextBucketAggregation.metadata
		}
		result[layer.nextBucketAggregation.name] = buckets
	}
	return result, nil
}

// valueForColumn returns value for a given column name in the first row of the result set (if it exists, it's the same for all rows)
func (p *pancakeJSONRenderer) valueForColumn(rows []model.QueryResultRow, columnName string) (value interface{}, found bool) {
	if len(rows) == 0 {
		return nil, false
	}
	for _, col := range rows[0].Cols {
		if col.ColName == columnName {
			return col.Value, true
		}
	}
	return nil, false
}

func (p *pancakeJSONRenderer) toJSON(agg *pancakeModel, rows []model.QueryResultRow) (model.JsonMap, error) {
	return p.layerToJSON(agg.layers, rows)
}
