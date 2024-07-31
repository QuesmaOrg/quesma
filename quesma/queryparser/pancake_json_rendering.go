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

func pancakeSelectMetricRows(name string, rows []model.QueryResultRow) []model.QueryResultRow {
	result := []model.QueryResultRow{}
	for i, row := range rows {
		newRow := model.QueryResultRow{Index: row.Index}
		for _, col := range row.Cols {
			if strings.HasPrefix(col.ColName, name) {
				newRow.Cols = append(newRow.Cols, col)
			}
		}
		result = append(result, newRow)
		if i == 0 {
			break // just one row please. FIXME Style. If here is only to fix staticcheck...
		}
	}
	return result
}

func pancakeSplitBucketRows(name string, rows []model.QueryResultRow) ([]model.QueryResultRow, [][]model.QueryResultRow) {

	buckets := []model.QueryResultRow{}
	subAggrs := [][]model.QueryResultRow{}
	if len(rows) == 0 {
		return buckets, subAggrs
	}
	indexName := rows[0].Index
	buckets = append(buckets, model.QueryResultRow{Index: indexName})
	subAggrs = append(subAggrs, []model.QueryResultRow{{Index: indexName}})
	for _, cols := range rows[0].Cols {

		if strings.HasPrefix(cols.ColName, name+"key") || strings.HasPrefix(cols.ColName, name+"count") {
			buckets[0].Cols = append(buckets[0].Cols, cols)
		} else {
			subAggrs[0][0].Cols = append(subAggrs[0][0].Cols, cols)
		}
	}
	// restRow
	for _, row := range rows[1:] {
		isNewBucket := false
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
				if strings.HasPrefix(cols.ColName, name+"key") || strings.HasPrefix(cols.ColName, name+"count") {
					buckets[lastIdx].Cols = append(buckets[lastIdx].Cols, cols)
				} else {
					subAggrs[lastIdx][0].Cols = append(subAggrs[lastIdx][0].Cols, cols)
				}
			}
		} else {
			lastIdx := len(buckets) - 1
			subAggrs[lastIdx] = append(subAggrs[lastIdx], model.QueryResultRow{Index: indexName})
			for _, cols := range row.Cols {
				if !(strings.HasPrefix(cols.ColName, name+"key") || strings.HasPrefix(cols.ColName, name+"count")) {
					lastSubIdx := len(subAggrs[lastIdx]) - 1
					subAggrs[lastIdx][lastSubIdx].Cols = append(subAggrs[lastIdx][lastSubIdx].Cols, cols)
				}
			}
		}
	}

	return buckets, subAggrs
}

func pancakeRenderJSONLayer(layerId int, layers []*pancakeAggregationLayer, rows []model.QueryResultRow) (model.JsonMap, error) {

	result := model.JsonMap{}
	if layerId >= len(layers) {
		return result, nil
	}
	layer := layers[layerId]
	for _, metric := range layer.currentMetricAggregations {
		metricName := "metric__"
		for i := 0; i < layerId; i++ {
			metricName = fmt.Sprintf("%s%s__", metricName, layers[i].nextBucketAggregation.name)
		}
		metricName = fmt.Sprintf("%s%s_col_", metricName, metric.name)
		metricRows := pancakeSelectMetricRows(metricName, rows)
		result[metric.name] = metric.queryType.TranslateSqlResponseToJson(metricRows, 0) // TODO: fill level?
	}

	if layer.nextBucketAggregation != nil {
		bucketName := "aggr__"
		for i := 0; i <= layerId; i++ {
			bucketName = fmt.Sprintf("%s%s__", bucketName, layers[i].nextBucketAggregation.name)
		}
		bucketRows, subAggrRows := pancakeSplitBucketRows(bucketName, rows)

		// We are filter out null
		if layer.nextBucketAggregation.whereClause != nil {
			// TODO: nicer way of passing not null
			if _, ok := layer.nextBucketAggregation.whereClause.(model.InfixExpr); ok {
				nullRowToDelete := -1
				nameofKey := bucketName + "key"
			ROW:
				for i, row := range bucketRows {
					for _, col := range row.Cols {
						if strings.HasPrefix(col.ColName, nameofKey) {
							if col.Value == nil || col.Value == "" {
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
		}
		buckets := layer.nextBucketAggregation.queryType.TranslateSqlResponseToJson(bucketRows, 0) // TODO: fill level?

		if layerId+1 < len(layers) { // Add subAggregation
			if bucketArrRaw, ok := buckets["buckets"]; ok {
				bucketArr := bucketArrRaw.([]model.JsonMap)
				if len(bucketArr) != len(subAggrRows) {
					// TODO: Maybe handle it somehow
					return nil, fmt.Errorf("buckets and subAggrRows should have the same length. layer: %s ", layer.nextBucketAggregation.name)
				}

				for i, bucket := range bucketArr {
					subAggr, err := pancakeRenderJSONLayer(layerId+1, layers, subAggrRows[i])
					if err != nil {
						return nil, err
					}
					bucketArr[i] = util.MergeMaps(context.Background(), bucket, subAggr, model.KeyAddedByQuesma)
				}
			} else {
				return nil, fmt.Errorf("no buckets key in bucket json, layer: %s", layer.nextBucketAggregation.name)
			}
		}

		result[layer.nextBucketAggregation.name] = buckets
	}
	return result, nil
}

func pancakeRenderJSON(agg *pancakeAggregation, rows []model.QueryResultRow) (model.JsonMap, error) {
	return pancakeRenderJSONLayer(0, agg.layers, rows)
}
