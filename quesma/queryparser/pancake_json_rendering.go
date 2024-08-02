// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"quesma/model"
	"quesma/util"
	"strings"
)

type pancakeJSONRenderer struct {
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

func (p *pancakeJSONRenderer) splitBucketRows(bucket *pancakeModelBucketAggregation, rows []model.QueryResultRow) (
	buckets []model.QueryResultRow, subAggrs [][]model.QueryResultRow) {

	if len(rows) == 0 {
		return buckets, subAggrs
	}
	bucketKeyName := bucket.InternalNameForKeyPrefix()
	bucketCountName := bucket.InternalNameForCount()
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
				if strings.HasPrefix(cols.ColName, bucketKeyName) || strings.HasPrefix(cols.ColName, bucketCountName) {
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
func (p *pancakeJSONRenderer) potentiallyRemoveExtraBucket(layer *pancakeModelLayer, bucketRows []model.QueryResultRow, subAggrRows [][]model.QueryResultRow) ([]model.QueryResultRow, [][]model.QueryResultRow) {
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

func (p *pancakeJSONRenderer) layerToJSON(layerIdx int, layers []*pancakeModelLayer, rows []model.QueryResultRow) (model.JsonMap, error) {

	result := model.JsonMap{}
	if layerIdx >= len(layers) {
		return result, nil
	}
	layer := layers[layerIdx]
	for _, metric := range layer.currentMetricAggregations {
		metricRows := p.selectMetricRows(metric.internalName+"_col_", rows)
		result[metric.name] = metric.queryType.TranslateSqlResponseToJson(metricRows, 0) // TODO: fill level?
		// maybe add metadata also here? probably not needed
	}

	if layer.nextBucketAggregation != nil {
		bucketRows, subAggrRows := p.splitBucketRows(layer.nextBucketAggregation, rows)

		bucketRows, subAggrRows = p.potentiallyRemoveExtraBucket(layer, bucketRows, subAggrRows)
		buckets := layer.nextBucketAggregation.queryType.TranslateSqlResponseToJson(bucketRows, layerIdx+1) // TODO: for date_histogram this layerIdx+1 layer seems correct, is it for all?

		if len(buckets) == 0 { // without this we'd generate {"buckets": []} in the response, which Elastic doesn't do.
			if layer.nextBucketAggregation.metadata != nil {
				buckets["meta"] = layer.nextBucketAggregation.metadata
				result[layer.nextBucketAggregation.name] = buckets
			}
			return result, nil
		}

		if layerIdx+1 < len(layers) { // Add subAggregation
			if bucketArrRaw, ok := buckets["buckets"]; ok {
				bucketArr := bucketArrRaw.([]model.JsonMap)
				if len(bucketArr) != len(subAggrRows) {
					// TODO: Maybe handle it somehow
					return nil, fmt.Errorf("buckets and subAggrRows should have the same length. layer: %s ", layer.nextBucketAggregation.name)
				}

				for i, bucket := range bucketArr {
					// TODO: Maybe add model.KeyAddedByQuesma if there are more than one pancake
					subAggr, err := p.layerToJSON(layerIdx+1, layers, subAggrRows[i])
					if err != nil {
						return nil, err
					}
					bucketArr[i] = util.MergeMaps(context.Background(), bucket, subAggr, model.KeyAddedByQuesma)
				}
			} else {
				return nil, fmt.Errorf("no buckets key in bucket json, layer: %s", layer.nextBucketAggregation.name)
			}
		}

		if layer.nextBucketAggregation.metadata != nil {
			buckets["meta"] = layer.nextBucketAggregation.metadata
		}
		result[layer.nextBucketAggregation.name] = buckets
	}
	return result, nil
}

func (p *pancakeJSONRenderer) toJSON(agg *pancakeModel, rows []model.QueryResultRow) (model.JsonMap, error) {
	return p.layerToJSON(0, agg.layers, rows)
}
