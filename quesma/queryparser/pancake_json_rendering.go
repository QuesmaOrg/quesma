// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"fmt"
	"github.com/k0kubun/pp"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/util"
	"strings"
)

type pancakeJSONRenderer struct {
	ctx                                      context.Context
	pipelineAggregationToResultBucketPointer map[string]*model.JsonMap
	pipelineAggregationFullToShortName       map[string]string
}

func newPancakeJSONRenderer(ctx context.Context) *pancakeJSONRenderer {
	return &pancakeJSONRenderer{
		ctx:                                      ctx,
		pipelineAggregationToResultBucketPointer: make(map[string]*model.JsonMap, 0), // TODO combine to 1 map maybe
		pipelineAggregationFullToShortName:       make(map[string]string, 0),
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

func (p *pancakeJSONRenderer) selectPrefixRows(prefix string, rows []model.QueryResultRow) (result []model.QueryResultRow) {
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

func (p *pancakeJSONRenderer) selectPipelineRows(pipeline model.PipelineQueryType, rows []model.QueryResultRow) (result []model.QueryResultRow) {
	isCount := pipeline.IsCount()
	for _, row := range rows {
		newRow := model.QueryResultRow{Index: row.Index}
		for _, col := range row.Cols {
			if !isCount && strings.HasSuffix(col.ColName, "count") {
				continue
			}
			if !strings.HasSuffix(col.ColName, "order_1") {
				newRow.Cols = append(newRow.Cols, col)
			}
		}
		result = append(result, newRow)
	}
	return
}

func (p *pancakeJSONRenderer) splitBucketRows(bucket *pancakeModelBucketAggregation, rows []model.QueryResultRow) (
	buckets []model.QueryResultRow, subAggrs [][]model.QueryResultRow, rowIndexes []int) {

	if len(rows) == 0 {
		return buckets, subAggrs, rowIndexes
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
			rowIndexes = append(rowIndexes, rowIdx)
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

	return buckets, subAggrs, rowIndexes
}

// In some queries we want to filter out null values or empty
// We accomplish that by increasing limit by one during SQL query and then filtering out during JSON rendering.
// So we either filter out empty or last one if there is none.
// This can't be replaced by WHERE in generic case.
func (p *pancakeJSONRenderer) potentiallyRemoveExtraBucket(layer *pancakeModelLayer, bucketRows []model.QueryResultRow,
	subAggrRows [][]model.QueryResultRow, rowIndexes []int) ([]model.QueryResultRow, [][]model.QueryResultRow, []int) {
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
			rowIndexes = append(rowIndexes[:nullRowToDelete], rowIndexes[nullRowToDelete+1:]...)
		} else if layer.nextBucketAggregation.limit != 0 && len(bucketRows) > layer.nextBucketAggregation.limit {
			bucketRows = bucketRows[:layer.nextBucketAggregation.limit]
			subAggrRows = subAggrRows[:layer.nextBucketAggregation.limit]
			rowIndexes = rowIndexes[:layer.nextBucketAggregation.limit]
		}
	}
	return bucketRows, subAggrRows, rowIndexes
}

func (p *pancakeJSONRenderer) processPip(result model.JsonMap, layer *pancakeModelLayer,
	pipeline *pancakeModelPipelineAggregation, rows []model.QueryResultRow) model.JsonMap {
	pp.Println("processPip, pipeline, name:", pipeline.internalName, "rows", rows)
	processedRows := pipeline.queryType.CalculateResultWhenMissing(p.selectPipelineRows(pipeline.queryType, rows))
	fmt.Println("processedRows", processedRows)
	result[pipeline.name] = pipeline.queryType.TranslateSqlResponseToJson(processedRows, 0) // TODO: fill level?
	p.pipelineAggregationFullToShortName[pipeline.internalName] = pipeline.name
	p.pipelineAggregationToResultBucketPointer[pipeline.internalName] = &result

	for _, pipelineChild := range layer.findPipelineChildren(pipeline) {
		result = p.processPip(result, layer, pipelineChild, processedRows)
	}

	return result
}

func (p *pancakeJSONRenderer) processPip2(layer *pancakeModelLayer, pipeline *pancakeModelPipelineAggregation, bucketRows []model.QueryResultRow,
	pipelineRows *[][]model.QueryResultRow, pipelineNames *[]string, pipelineTypes *[]model.AggregationType) {

	thisPipelineRows := p.pipelineToJSON(pipeline, bucketRows)

	fmt.Println("thisPipelineRows", thisPipelineRows)

	*pipelineRows = append(*pipelineRows, thisPipelineRows)
	*pipelineNames = append(*pipelineNames, pipeline.name)
	*pipelineTypes = append(*pipelineTypes, pipeline.queryType.PipelineAggregationType())

	for _, pipelineChild := range layer.findPipelineChildren(pipeline) {
		p.processPip2(layer, pipelineChild, thisPipelineRows, pipelineRows, pipelineNames, pipelineTypes)
	}
}

func (p *pancakeJSONRenderer) combinatorBucketToJSON(remainingLayers []*pancakeModelLayer, rows []model.QueryResultRow) (model.JsonMap, error) {
	layer := remainingLayers[0]
	switch queryType := layer.nextBucketAggregation.queryType.(type) {
	case bucket_aggregations.SamplerInterface, bucket_aggregations.FilterAgg:
		selectedRows := p.selectMetricRows(layer.nextBucketAggregation.InternalNameForCount(), rows)
		aggJson := layer.nextBucketAggregation.queryType.TranslateSqlResponseToJson(selectedRows, 0)
		subAggr, err := p.layerToJSON(remainingLayers[1:], rows)
		if err != nil {
			return nil, err
		}
		return util.MergeMaps(p.ctx, aggJson, subAggr, model.KeyAddedByQuesma), nil
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

			bucketArray = append(bucketArray,
				util.MergeMaps(p.ctx, aggJson, subAggr, model.KeyAddedByQuesma))
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
		pp.Println("layerToJSON, metric")
		metricRows := p.selectMetricRows(metric.internalName+"_col_", rows)
		result[metric.name] = metric.queryType.TranslateSqlResponseToJson(metricRows, 0) // TODO: fill level?
		// TODO: maybe add metadata also here? probably not needed
	}

	for _, pipeline := range layer.childrenPipelineAggregations {
		// TODO: !!
		if pipeline.internalName == "pipeline__2__1" {
			continue
		}
		pp.Println("1", pipeline.internalName, pipeline.parentColumnName(p.ctx))
		result = p.processPip(result, layer, pipeline, rows)
	}
	// od razu

	fmt.Println("rows na poczatku", rows)

	if layer.nextBucketAggregation != nil {
		// sampler and filter are special
		if !layer.nextBucketAggregation.DoesHaveGroupBy() {
			json, err := p.combinatorBucketToJSON(remainingLayers, rows)
			if err != nil {
				return nil, err
			}
			result[layer.nextBucketAggregation.name] = json
			return result, nil
		}

		fmt.Printf("nextBucketAggr: %+v\n\n, wchodzimy? %d %d", layer.nextBucketAggregation, len(remainingLayers))
		if len(remainingLayers) > 1 {
			nextLayer := remainingLayers[1]
			for _, pipeline := range nextLayer.childrenPipelineAggregations {
				fmt.Println("pipeline iteration...")
				//result[pipeline.name] = pipeline.queryType.TranslateSqlResponseToJson(rows, 0) // TODO: fill level?
				p.pipelineAggregationFullToShortName[pipeline.internalName] = pipeline.name
				p.pipelineAggregationToResultBucketPointer[pipeline.internalName] = &result
			}
		}

		bucketRows, subAggrRows, rowIndexes := p.splitBucketRows(layer.nextBucketAggregation, rows)
		bucketRows, subAggrRows, rowIndexes = p.potentiallyRemoveExtraBucket(layer, bucketRows, subAggrRows, rowIndexes)

		fmt.Println("bucket rowsy:", bucketRows)
		fmt.Println()
		//fmt.Println("subaggr rowsy:", subAggrRows)

		var pipelineRows [][]model.QueryResultRow
		var pipelineNames []string
		var pipelineTypes []model.AggregationType
		if len(remainingLayers) > 1 {
			nextLayer := remainingLayers[1]
			for _, childPipeline := range nextLayer.childrenPipelineAggregations {
				if childPipeline.internalName == "pipeline__2__1" { // TODO: fix
					continue
				}
				var oldColumnArr []any
				fmt.Printf("child pipeline: %+v\ncur aggr: %+v\n", childPipeline, layer.nextBucketAggregation.queryType)
				needToAddProperMetricColumn := !childPipeline.queryType.IsCount() // If count, last column of bucketRows is already count we need.
				if needToAddProperMetricColumn {
					fmt.Println("-- rendering: adding Column, bucket rows:", bucketRows)
					bucketRows, oldColumnArr = p.addColumn(childPipeline.parentColumnName(p.ctx), bucketRows, rows, rowIndexes)
					fmt.Println("== rendering: added Column, bucket rows:", bucketRows)
				}

				// hmm TODO: find a nicer way
				if histogram, ok := layer.nextBucketAggregation.queryType.(bucket_aggregations.Histogram); ok {
					bucketRowsTransformed := histogram.NewRowsTransformer().Transform(p.ctx, bucketRows)
					p.processPip2(nextLayer, childPipeline, bucketRowsTransformed, &pipelineRows, &pipelineNames, &pipelineTypes)
				} else {
					p.processPip2(nextLayer, childPipeline, bucketRows, &pipelineRows, &pipelineNames, &pipelineTypes)
				}

				pipelineNames = append(pipelineNames, childPipeline.name)
				pipelineTypes = append(pipelineTypes, childPipeline.queryType.PipelineAggregationType())
				if needToAddProperMetricColumn {
					bucketRows = p.restoreLastColumn(bucketRows, oldColumnArr)
				}
			}
		}

		bucketRows, subAggrRows, _ = p.potentiallyRemoveExtraBucket(layer, bucketRows, subAggrRows)
		buckets := layer.nextBucketAggregation.queryType.TranslateSqlResponseToJson(bucketRows, 0)

		if len(buckets) == 0 { // without this we'd generate {"buckets": []} in the response, which Elastic doesn't do.
			if layer.nextBucketAggregation.metadata != nil {
				buckets["meta"] = layer.nextBucketAggregation.metadata
				result[layer.nextBucketAggregation.name] = buckets
			}
			return result, nil
		}

		if len(remainingLayers) > 1 { // Add subAggregation
			if bucketArrRaw, ok := buckets["buckets"]; ok {
				fmt.Println("bucketArrRaw", bucketArrRaw)
				bucketArr := bucketArrRaw.([]model.JsonMap)

				fmt.Println("bucket, lens:", len(bucketArr), len(subAggrRows))
				if len(bucketArr) == len(subAggrRows) {

					// Simple case, we merge bucketArr[i] with subAggrRows[i] (if lengths are equal, keys must be equal => it's fine to not check them at all)
					for i, bucket := range bucketArr {
						if docCount, ok := bucket["doc_count"]; ok {
							if fmt.Sprintf("%v", docCount) == "0" { // Not sure, but it does the trick
								continue
							}
						}
						// TODO: Maybe add model.KeyAddedByQuesma if there are more than one pancake
						subAggr, err := p.layerToJSON(remainingLayers[1:], subAggrRows[i])
						if err != nil {
							return nil, err
						}
						bucketArr[i] = util.MergeMaps(p.ctx, bucket, subAggr, model.KeyAddedByQuesma)
						for j, singlePipelineRows := range pipelineRows {
							if pipelineTypes[j] == model.BucketAggregation {
								bucketArr[i][pipelineNames[j]] = model.JsonMap{"value": singlePipelineRows[i].LastColValue()}
							}
						}
					}
				} else {
					// A bit harder case. Observation: len(bucketArr) > len(subAggrRows) and set(subAggrRows' keys) is a subset of set(bucketArr's keys)
					// So if bucket[i]'s key corresponds to subAggr[subAggrIdx]'s key, we merge them.
					// If not, we just keep bucket[i] (i++, subAggrIdx stays the same)
					subAggrIdx := 0
					for i, bucket := range bucketArr {
						if docCount, ok := bucket["doc_count"]; ok {
							if fmt.Sprintf("%v", docCount) == "0" { // Not sure, but it does the trick
								continue
							}
						}
						key, exists := bucket["key"]
						if !exists {
							return nil, fmt.Errorf("no key in bucket json, layer: %s", layer.nextBucketAggregation.name)
						}

						columnNameWithKey := layer.nextBucketAggregation.InternalNameForKey(0) // TODO: need all ids, multi_terms will probably not work now
						subAggrKey, found := p.valueForColumn(subAggrRows[subAggrIdx], columnNameWithKey)
						if found && subAggrKey == key {
							subAggr, err := p.layerToJSON(remainingLayers[1:], subAggrRows[subAggrIdx])
							if err != nil {
								return nil, err
							}
							bucketArr[i] = util.MergeMaps(p.ctx, bucket, subAggr, model.KeyAddedByQuesma)
							subAggrIdx++
						} else {
							bucketArr[i] = bucket
						}

						for j, singlePipelineRows := range pipelineRows {
							if pipelineTypes[j] == model.BucketAggregation {
								bucketArr[i][pipelineNames[j]] = model.JsonMap{"value": singlePipelineRows[i].LastColValue()}
							}
						}
					}
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

func (p *pancakeJSONRenderer) pipelineToJSON(pipeline *pancakeModelPipelineAggregation, rows []model.QueryResultRow) []model.QueryResultRow {
	fmt.Println("hoho, pipelineToJSON", rows)
	fmt.Println("wynik:", pipeline.queryType.TranslateSqlResponseToJson(rows, 0))
	pp.Println(p.pipelineAggregationToResultBucketPointer)
	//ptr := p.pipelineAggregationToResultBucketPointer[pipeline.internalName]

	pp.Println("ptr", pipeline.internalName)

	a, ok := pipeline.queryType.(model.PipelineQueryType)
	if !ok {
		fmt.Println("WTF")
		panic(nil)
	}
	b := a.CalculateResultWhenMissing(rows)
	if pipeline.queryType.PipelineAggregationType() == model.MetricsAggregation {
		pp.Println("DODAJE DO RESPONSE")
		//(*ptr)[pipeline.name] = pipeline.queryType.TranslateSqlResponseToJson(b, 0)
	}
	fmt.Println("KK rows po calculate:", a.CalculateResultWhenMissing(rows), "pipeline jest metryczna?",
		pipeline.queryType.PipelineAggregationType() == model.MetricsAggregation, "jak tak, to właśnie dodałem do response, jak nie to nie :/")
	return b
}

func (p *pancakeJSONRenderer) addColumn(
	parentColumnName string, selectedRows, allRows []model.QueryResultRow, selectedRowsIndexes []int) (
	newSelectedRows []model.QueryResultRow, oldColumnArray []any) {
	fmt.Println("hoho, addColumn", parentColumnName)
	if len(allRows) == 0 {
		return
	}

	for _, col := range allRows[0].Cols {
		fmt.Println("col:", col.ColName)
	}

	colIdx := -1
	for i, col := range allRows[0].Cols {
		if col.ColName == parentColumnName {
			colIdx = i
			break
		}
	}
	fmt.Println("DD", parentColumnName, colIdx)

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
			// newSelectedRows[i].Cols = append(newSelectedRows[i].Cols, allRows[selectedRowsIndexes[i]].Cols[colIdx])
		}
	}

	return
}

func (p *pancakeJSONRenderer) restoreLastColumn(rows []model.QueryResultRow, valuesToRestore []any) []model.QueryResultRow {
	for i, row := range rows {
		row.Cols[len(row.Cols)-1].Value = valuesToRestore[i] // used to be -2
	}
	return rows
}

func (p *pancakeJSONRenderer) toJSON(agg *pancakeModel, rows []model.QueryResultRow) (model.JsonMap, error) {
	return p.layerToJSON(agg.layers, rows)
}
