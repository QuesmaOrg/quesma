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

func pancakeSplitBucketRows(name string, rows []model.QueryResultRow) ([]model.QueryResultRow, [][]model.QueryResultRow) {
	// pp.Println("JM: pancakeSplitBucketRows", name)
	buckets := []model.QueryResultRow{}
	subAggrs := [][]model.QueryResultRow{}
	if len(rows) == 0 {
		return buckets, subAggrs
	}
	indexName := rows[0].Index
	buckets = append(buckets, model.QueryResultRow{Index: indexName})
	subAggrs = append(subAggrs, []model.QueryResultRow{model.QueryResultRow{Index: indexName}})
	for _, cols := range rows[0].Cols {
		// pp.Println("JM: pancakeSplitBucketRows first", name, cols.ColName)
		if strings.HasPrefix(cols.ColName, name+"key") || strings.HasPrefix(cols.ColName, name+"order") {
			buckets[0].Cols = append(buckets[0].Cols, cols)
		} else {
			subAggrs[0][0].Cols = append(subAggrs[0][0].Cols, cols)
		}
	}
	// restRow
	for _, row := range rows[1:] {
		isNewBucket := false
		//pp.Println("JM: row", row)
		//pp.Println("JM: bucket", buckets)
		previousBucket := buckets[len(buckets)-1]
		for _, cols := range row.Cols {
			if strings.HasPrefix(cols.ColName, name+"key") {
				noSameKeyValue := true
				for _, previousCols := range previousBucket.Cols {
					if cols.ColName == previousCols.ColName {
						if cols.Value == previousCols.Value {
							noSameKeyValue = false
						}
						break
					}
				}
				if noSameKeyValue {
					isNewBucket = true
					break
				}
			}
		}

		// check if it's a new bucket
		if isNewBucket {
			buckets = append(buckets, model.QueryResultRow{Index: indexName})
			subAggrs = append(subAggrs, []model.QueryResultRow{model.QueryResultRow{Index: indexName}})
			lastIdx := len(buckets) - 1
			for _, cols := range row.Cols {
				if strings.HasPrefix(cols.ColName, name+"key") || strings.HasPrefix(cols.ColName, name+"order") {
					buckets[lastIdx].Cols = append(buckets[lastIdx].Cols, cols)
				} else {
					subAggrs[lastIdx][0].Cols = append(subAggrs[lastIdx][0].Cols, cols)
				}
			}
		} else {
			lastIdx := len(buckets) - 1
			subAggrs[lastIdx] = append(subAggrs[lastIdx], model.QueryResultRow{Index: indexName})
			for _, cols := range row.Cols {
				if !(strings.HasPrefix(cols.ColName, name+"_key") || strings.HasPrefix(cols.ColName, name+" _order")) {
					lastSubIdx := len(subAggrs[lastIdx]) - 1
					subAggrs[lastIdx][lastSubIdx].Cols = append(subAggrs[lastIdx][lastSubIdx].Cols, cols)
				}
			}
		}
	}

	// pp.Println("JM: pancakeSplitBucketRows result", name, buckets, subAggrs)
	return buckets, subAggrs
}

func pancakeRenderJSONLayer(layerId int, layers []*pancakeAggregationLayer, rows []model.QueryResultRow) model.JsonMap {
	result := model.JsonMap{}
	if layerId >= len(layers) {
		return result
	}
	layer := layers[layerId]
	for _, metric := range layer.currentMetricAggregations {
		result[metric.name] = model.JsonMap{
			"aggregation": "metric",
		}
	}

	if layer.nextBucketAggregation != nil {
		bucketName := "aggr__"
		for i := 0; i <= layerId; i++ {
			bucketName = fmt.Sprintf("%s%s__", bucketName, layers[i].nextBucketAggregation.name)
		}
		bucketRows, subAggrRows := pancakeSplitBucketRows(bucketName, rows)
		buckets := layer.nextBucketAggregation.queryType.TranslateSqlResponseToJson(bucketRows, 0) // TODO: fill level?

		if layerId+1 < len(layers) { // Add subAggregation
			if bucketArrRaw, ok := buckets["buckets"]; ok {
				bucketArr := bucketArrRaw.([]model.JsonMap)
				if len(buckets) != len(subAggrRows) {
					// TODO: Maybe handle it somehow
					// pp.Println("JM: pancakeRenderJSONLayer mismatch count", len(buckets), len(subAggrRows))
					panic("buckets and subAggrRows should have the same length")
				}

				for i, bucket := range bucketArr {
					subAggr := pancakeRenderJSONLayer(layerId+1, layers, subAggrRows[i])
					bucketArr[i] = util.MergeMaps(context.Background(), bucket, subAggr, model.KeyAddedByQuesma)
				}
			} else {
				panic("no buckets key in bucket json")
			}
		}

		result[layer.nextBucketAggregation.name] = buckets
	}
	return result
}

func pancakeRenderJSON(agg *pancakeAggregation, rows []model.QueryResultRow) model.JsonMap {
	return pancakeRenderJSONLayer(0, agg.layers, rows)
}
