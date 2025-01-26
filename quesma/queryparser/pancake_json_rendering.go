// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/model/bucket_aggregations"
	"github.com/QuesmaOrg/quesma/quesma/model/metrics_aggregations"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"math/big"
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
	return
}

// selectMetricRowsMultipleNames: exactly like selectMetricRows above, but for multiple metric names.
func (p *pancakeJSONRenderer) selectMetricRowsMultipleNames(metricNames []string, rows []model.QueryResultRow) (result []model.QueryResultRow) {
	if len(rows) > 0 {
		newRow := model.QueryResultRow{Index: rows[0].Index}
		for _, col := range rows[0].Cols {
			for _, name := range metricNames {
				if strings.HasPrefix(col.ColName, name) {
					newRow.Cols = append(newRow.Cols, col)
					break
				}
			}
		}
		return []model.QueryResultRow{newRow}
	}
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
							var isEqual bool
							switch val := cols.Value.(type) {
							case big.Int:
								prevVal := previousCols.Value.(big.Int)
								isEqual = val.Cmp(&prevVal) == 0
							default:
								isEqual = val == previousCols.Value
							}
							if !isEqual {
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
		aggJson := layer.nextBucketAggregation.queryType.TranslateSqlResponseToJson(selectedRows)
		subAggr, err := p.layerToJSON(remainingLayers[1:], rows)
		if err != nil {
			return nil, err
		}
		mergeResult, mergeErr := util.MergeMaps(aggJson, subAggr)
		if mergeErr != nil {
			logger.ErrorWithCtx(p.ctx).Msgf("error merging maps: %v", mergeErr)
		}
		return mergeResult, nil
	case bucket_aggregations.CombinatorAggregationInterface:
		var bucketArray []model.JsonMap
		for _, subGroup := range queryType.CombinatorGroups() {
			selectedRowsWithoutPrefix := p.selectPrefixRows(subGroup.Prefix, rows)

			subAggr, err := p.layerToJSON(remainingLayers[1:], selectedRowsWithoutPrefix)
			if err != nil {
				return nil, err
			}

			selectedRows := p.selectMetricRows(layer.nextBucketAggregation.InternalNameForCount(), selectedRowsWithoutPrefix)
			aggJson := queryType.CombinatorTranslateSqlResponseToJson(subGroup, selectedRows)

			mergeResult, mergeErr := util.MergeMaps(aggJson, subAggr)
			if mergeErr != nil {
				logger.ErrorWithCtx(p.ctx).Msgf("error merging maps: %v", mergeErr)
			}
			bucketArray = append(bucketArray, mergeResult)
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
	fmt.Println("kk model:", rows)
	for _, metric := range layer.currentMetricAggregations {
		var metricRows []model.QueryResultRow
		switch metric.queryType.(type) {
		case *metrics_aggregations.TopMetrics, *metrics_aggregations.TopHits:
			metricRows = p.selectTopHitsRows(metric, rows)
		case *metrics_aggregations.Rate:
			// Special, as we need to select also parent date_histogram's values.

			// 2 lines below: e.g. metric__2__year -> aggr__2
			parentHistogramColName := fmt.Sprintf("aggr%s", strings.TrimPrefix(metric.internalName, "metric"))
			parentHistogramColName = strings.TrimSuffix(parentHistogramColName, metric.name)

			var (
				parentHistogramKey = fmt.Sprintf("%skey_0", parentHistogramColName)
				metricValue        string
			)
			rate, _ := metric.queryType.(*metrics_aggregations.Rate)
			if rate.FieldPresent() {
				// if we have field, we use it
				metricValue = metric.InternalNamePrefix()
			} else {
				// else: our value is date_histogram's count
				metricValue = fmt.Sprintf("%scount", parentHistogramColName)
			}
			metricRows = p.selectMetricRowsMultipleNames([]string{parentHistogramKey, metricValue}, rows)

		default:
			metricRows = p.selectMetricRows(metric.InternalNamePrefix(), rows)
		}
		if metric.name != PancakeTotalCountMetricName {
			result[metric.name] = metric.queryType.TranslateSqlResponseToJson(metricRows)
		}
		// TODO: maybe add metadata also here? probably not needed
	}

	// pipeline aggregations of metric type behave just like metric
	for metricPipelineAggrName, metricPipelineAggrResult := range p.pipeline.currentPipelineMetricAggregations(layer, rows) {
		result[metricPipelineAggrName] = metricPipelineAggrResult
		// TODO: maybe add metadata also here? probably not needed
	}

	if layer.nextBucketAggregation != nil {
		// sampler and filter are special
		if !layer.nextBucketAggregation.DoesHaveGroupBy() {
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
		bucketRows, subAggrRows = p.potentiallyRemoveExtraBucket(layer, bucketRows, subAggrRows)

		buckets := layer.nextBucketAggregation.queryType.TranslateSqlResponseToJson(bucketRows)

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

			if len(bucketArr) == len(subAggrRows) {
				// Simple case, we merge bucketArr[i] with subAggrRows[i] (if lengths are equal, keys must be equal => it's fine to not check them at all)
				for i, bucket := range bucketArr {
					for pipelineAggrName, pipelineAggrResult := range pipelineBucketsPerAggregation {
						bucketArr[i][pipelineAggrName] = pipelineAggrResult[i]
					}

					subAggr, err := p.layerToJSON(remainingLayers[1:], subAggrRows[i])
					if err != nil {
						return nil, err
					}
					if bucketArr[i], err = util.MergeMaps(bucket, subAggr); err != nil {
						logger.ErrorWithCtx(p.ctx).Msgf("error merging maps: %v", err)
					}
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

					// if our bucket aggregation is a date_histogram, we need original key, not processed one, which is "key"
					key, exists := bucket[bucket_aggregations.OriginalKeyName]
					if !exists {
						key, exists = bucket["key"]
						if !exists {
							return nil, fmt.Errorf("no key in bucket json, layer: %s", layer.nextBucketAggregation.name)
						}
					}
					var (
						columnNameWithKey        = layer.nextBucketAggregation.InternalNameForKey(0) // TODO: need all ids, multi_terms will probably not work now
						found                    bool
						subAggrKey               any
						currentBucketSubAggrRows []model.QueryResultRow
					)
					if subAggrIdx < len(subAggrRows) {
						subAggrKey, found = p.valueForColumn(subAggrRows[subAggrIdx], columnNameWithKey)
					}

					if found && subAggrKey == key {
						currentBucketSubAggrRows = subAggrRows[subAggrIdx]
						subAggrIdx++
					} else {
						currentBucketSubAggrRows = []model.QueryResultRow{}
					}

					subAggr, err := p.layerToJSON(remainingLayers[1:], currentBucketSubAggrRows)
					if err != nil {
						return nil, err
					}
					if bucketArr[i], err = util.MergeMaps(bucket, subAggr); err != nil {
						logger.ErrorWithCtx(p.ctx).Msgf("error merging maps: %v", err)
					}
				}
			}

			for i := 0; i < len(bucketArr); i++ {
				delete(bucketArr[i], bucket_aggregations.OriginalKeyName)
			}
		}

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
