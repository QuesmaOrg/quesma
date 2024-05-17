package queryparser

import (
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/model/pipeline_aggregations"
)

// CAUTION: maybe "return" everywhere isn't corrent, as maybe there can be multiple pipeline aggregations at one level.
// But I've tested some complex queries and it seems to not be the case. So let's keep it this way for now.
func (cw *ClickhouseQueryTranslator) parsePipelineAggregations(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	if aggregationType, success = cw.parseBucketScriptBasic(queryMap); success {
		delete(queryMap, "bucket_script")
		return
	}
	if aggregationType, success = cw.parseCumulativeSum(queryMap); success {
		delete(queryMap, "cumulative_sum")
		return
	}
	if aggregationType, success = cw.parseDerivative(queryMap); success {
		delete(queryMap, "derivative")
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

func (cw *ClickhouseQueryTranslator) parseDerivative(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	derivativeRaw, exists := queryMap["derivative"]
	if !exists {
		return
	}

	derivative, ok := derivativeRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("derivative is not a map, but %T, value: %v", derivativeRaw, derivativeRaw)
		return
	}
	bucketsPathRaw, exists := derivative["buckets_path"]
	if !exists {
		logger.WarnWithCtx(cw.Ctx).Msg("no buckets_path in derivative")
		return
	}
	bucketsPath, ok := bucketsPathRaw.(string)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("buckets_path is not a string, but %T, value: %v", bucketsPathRaw, bucketsPathRaw)
		return
	}
	return pipeline_aggregations.NewDerivative(cw.Ctx, bucketsPath), true
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

func (b *aggrQueryBuilder) buildPipelineAggregation(aggregationType model.QueryType, metadata model.JsonMap) model.Query {
	query := b.buildAggregationCommon(metadata)
	query.Type = aggregationType
	switch aggrType := aggregationType.(type) {
	case pipeline_aggregations.BucketScript:
		query.NonSchemaFields = append(query.NonSchemaFields, "count()")
	case pipeline_aggregations.CumulativeSum:
		query.NoDBQuery = true
		if aggrType.IsCount {
			query.NonSchemaFields = append(query.NonSchemaFields, "count()")
			if len(query.Aggregators) < 2 {
				logger.WarnWithCtx(b.ctx).Msg("cumulative_sum with count as parent, but no parent aggregation found")
			}
			query.Parent = query.Aggregators[len(query.Aggregators)-2].Name
		} else {
			query.Parent = aggrType.Parent
		}
	case pipeline_aggregations.Derivative:
		query.NoDBQuery = true
		if aggrType.IsCount {
			query.NonSchemaFields = append(query.NonSchemaFields, "count()")
			if len(query.Aggregators) < 2 {
				logger.WarnWithCtx(b.ctx).Msg("cumulative_sum with count as parent, but no parent aggregation found")
			}
			query.Parent = query.Aggregators[len(query.Aggregators)-2].Name
		} else {
			query.Parent = aggrType.Parent
		}
	}
	return query
}
