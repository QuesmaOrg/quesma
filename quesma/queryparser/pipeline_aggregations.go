package queryparser

import (
	"fmt"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/pipeline_aggregations"
)

func (cw *ClickhouseQueryTranslator) parsePipelineAggregations(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	if aggregationType, success = cw.parseBucketScriptBasic(queryMap); success {
		delete(queryMap, "bucket_script")
		return
	}
	if aggregationType, success = cw.parseCumulativeSum(queryMap); success {
		delete(queryMap, "cumulative_sum")
		return
	}
	return
}

func (cw *ClickhouseQueryTranslator) parseCumulativeSum(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	cumulativeSumRaw, exists := queryMap["cumulative_sum"]
	if !exists {
		return
	}

	cumulativeSum, ok := cumulativeSumRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("cumulative_sum is not a map, but %T, value: %v", cumulativeSumRaw, cumulativeSumRaw)
		return
	}
	bucketsPathRaw, exists := cumulativeSum["buckets_path"]
	if !exists {
		logger.WarnWithCtx(cw.Ctx).Msg("no buckets_path in cumulative_sum")
		return
	}
	bucketsPath, ok := bucketsPathRaw.(string)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("buckets_path is not a string, but %T, value: %v", bucketsPathRaw, bucketsPathRaw)
		return
	}

	return pipeline_aggregations.NewCumulativeSum(cw.Ctx, bucketsPath), true
}

func (cw *ClickhouseQueryTranslator) parseBucketScriptBasic(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	bucketScriptRaw, exists := queryMap["bucket_script"]
	if !exists {
		return
	}

	// so far we only handle "count" here :D
	delete(queryMap, "bucket_script")
	bucketScript, ok := bucketScriptRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("bucket_script is not a map, but %T, value: %v. Skipping this aggregation", bucketScriptRaw, bucketScriptRaw)
		return
	}

	// if ["buckets_path"] != "_count", skip the aggregation
	if bucketsPathRaw, exists := bucketScript["buckets_path"]; exists {
		if bucketsPath, ok := bucketsPathRaw.(string); ok {
			if bucketsPath != "_count" {
				logger.WarnWithCtx(cw.Ctx).Msgf("buckets_path is not '_count', but %s. Skipping this aggregation", bucketsPath)
				return
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("buckets_path is not a string, but %T, value: %v. Skipping this aggregation", bucketsPathRaw, bucketsPathRaw)
			return
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msg("no buckets_path in bucket_script. Skipping this aggregation")
		return
	}

	// if ["script"]["source"] != "_value", skip the aggregation
	scriptRaw, exists := bucketScript["script"]
	if !exists {
		logger.WarnWithCtx(cw.Ctx).Msg("no script in bucket_script. Skipping this aggregation")
		return
	}
	script, ok := scriptRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("script is not a map, but %T, value: %v. Skipping this aggregation", scriptRaw, scriptRaw)
		return
	}
	if sourceRaw, exists := script["source"]; exists {
		if source, ok := sourceRaw.(string); ok {
			if source != "_value" {
				logger.WarnWithCtx(cw.Ctx).Msgf("source is not '_value', but %s. Skipping this aggregation", source)
				return
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("source is not a string, but %T, value: %v. Skipping this aggregation", sourceRaw, sourceRaw)
			return
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msg("no source in script. Skipping this aggregation")
		return
	}

	// okay, we've checked everything, it's indeed a simple count
	return pipeline_aggregations.NewBucketScript(cw.Ctx), true
}

func (b *aggrQueryBuilder) buildPipelineAggregation(aggregationType model.QueryType, metadata model.JsonMap) model.QueryWithAggregation {
	query := b.buildAggregationCommon(metadata)
	query.Type = aggregationType
	switch aggrType := aggregationType.(type) {
	case pipeline_aggregations.BucketScript:
		query.NonSchemaFields = append(query.NonSchemaFields, "count()")
	case pipeline_aggregations.CumulativeSum:
		query.NoDBQuery = true
		if aggrType.IsCount {
			query.NonSchemaFields = append(query.NonSchemaFields, "count()")
		} else {
			query.Parent = aggrType.Parent
		}
	}
	return query
}

func (cw *ClickhouseQueryTranslator) sortInTopologicalOrder(queries []model.QueryWithAggregation) []int {
	nameToIndex := make(map[string]int, len(queries))
	nameToParentName := make(map[string]string, len(queries))
	queryInDegree := make(map[string]int, len(queries))
	for i, query := range queries {
		nameToIndex[query.Name()] = i
		queryInDegree[query.Name()] = 0
		if query.HasParentAggregation() {
			nameToParentName[query.Name()] = query.Parent
		}
	}

	indexesSorted := make([]int, 0, len(queries))
	for _, query := range queries {
		if query.HasParentAggregation() {
			queryInDegree[query.Parent]++
		}
	}
	fmt.Println("queryInDegree", queryInDegree)
	for len(indexesSorted) < len(queries) {
		lenStart := len(indexesSorted)
		for aggrName, inDegree := range queryInDegree {
			if inDegree == 0 {
				indexesSorted = append(indexesSorted, nameToIndex[aggrName])
				if parentName, exists := nameToParentName[aggrName]; exists {
					queryInDegree[parentName]--
				}
				delete(queryInDegree, aggrName)
			}
		}
		lenEnd := len(indexesSorted)
		if lenEnd == lenStart {
			// without this check, we'd end up in an infinite loop
			logger.WarnWithCtx(cw.Ctx).Msgf("could not sort queries in topological order, indexesSorted: %v, queryInDegree: %v", indexesSorted, queryInDegree)
			break
		}
	}
	return indexesSorted
}
