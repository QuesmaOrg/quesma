// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/model/pipeline_aggregations"
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
	if aggregationType, success = cw.parseSerialDiff(queryMap); success {
		delete(queryMap, "derivative")
		return
	}
	if aggregationType, success = cw.parseAverageBucket(queryMap); success {
		delete(queryMap, "avg_bucket")
		return
	}
	if aggregationType, success = cw.parseMinBucket(queryMap); success {
		delete(queryMap, "min_bucket")
		return
	}
	if aggregationType, success = cw.parseMaxBucket(queryMap); success {
		delete(queryMap, "max_bucket")
		return
	}
	if aggregationType, success = cw.parseSumBucket(queryMap); success {
		delete(queryMap, "sum_bucket")
		return
	}
	return
}

func (cw *ClickhouseQueryTranslator) parseCumulativeSum(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	cumulativeSumRaw, exists := queryMap["cumulative_sum"]
	if !exists {
		return
	}
	bucketsPath, ok := cw.parseBucketsPath(cumulativeSumRaw, "cumulative_sum")
	if !ok {
		return
	}
	return pipeline_aggregations.NewCumulativeSum(cw.Ctx, bucketsPath), true
}

func (cw *ClickhouseQueryTranslator) parseDerivative(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	derivativeRaw, exists := queryMap["derivative"]
	if !exists {
		return
	}
	bucketsPath, ok := cw.parseBucketsPath(derivativeRaw, "derivative")
	if !ok {
		return
	}
	return pipeline_aggregations.NewDerivative(cw.Ctx, bucketsPath), true
}

func (cw *ClickhouseQueryTranslator) parseAverageBucket(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	avgBucketRaw, exists := queryMap["avg_bucket"]
	if !exists {
		return
	}
	bucketsPath, ok := cw.parseBucketsPath(avgBucketRaw, "avg_bucket")
	if !ok {
		return
	}
	return pipeline_aggregations.NewAverageBucket(cw.Ctx, bucketsPath), true
}

func (cw *ClickhouseQueryTranslator) parseMinBucket(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	minBucketRaw, exists := queryMap["min_bucket"]
	if !exists {
		return
	}
	bucketsPath, ok := cw.parseBucketsPath(minBucketRaw, "min_bucket")
	if !ok {
		return
	}
	return pipeline_aggregations.NewMinBucket(cw.Ctx, bucketsPath), true
}

func (cw *ClickhouseQueryTranslator) parseMaxBucket(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	maxBucketRaw, exists := queryMap["max_bucket"]
	if !exists {
		return
	}
	bucketsPath, ok := cw.parseBucketsPath(maxBucketRaw, "max_bucket")
	if !ok {
		return
	}
	return pipeline_aggregations.NewMaxBucket(cw.Ctx, bucketsPath), true
}

func (cw *ClickhouseQueryTranslator) parseSumBucket(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	sumBucketRaw, exists := queryMap["sum_bucket"]
	if !exists {
		return
	}
	bucketsPath, ok := cw.parseBucketsPath(sumBucketRaw, "sum_bucket")
	if !ok {
		return
	}
	return pipeline_aggregations.NewSumBucket(cw.Ctx, bucketsPath), true
}

func (cw *ClickhouseQueryTranslator) parseSerialDiff(queryMap QueryMap) (aggregationType model.QueryType, success bool) {
	serialDiffRaw, exists := queryMap["serial_diff"]
	if !exists {
		return
	}

	// buckets_path
	bucketsPath, ok := cw.parseBucketsPath(serialDiffRaw, "serial_diff")
	if !ok {
		return
	}

	// lag
	const defaultLag = 1
	serialDiff, ok := serialDiffRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("serial_diff is not a map, but %T, value: %v", serialDiffRaw, serialDiffRaw)
		return
	}
	lagRaw, exists := serialDiff["lag"]
	if !exists {
		return pipeline_aggregations.NewSerialDiff(cw.Ctx, bucketsPath, defaultLag), true
	}
	if lag, ok := lagRaw.(float64); ok {
		return pipeline_aggregations.NewSerialDiff(cw.Ctx, bucketsPath, int(lag)), true
	}

	logger.WarnWithCtx(cw.Ctx).Msgf("lag is not a float64, but %T, value: %v", lagRaw, lagRaw)
	return
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
	bucketsPath, ok := cw.parseBucketsPath(bucketScript, "bucket_script")
	if !ok {
		return
	}
	if bucketsPath != pipeline_aggregations.BucketsPathCount {
		logger.WarnWithCtx(cw.Ctx).Msgf("buckets_path is not '_count', but %s. Skipping this aggregation", bucketsPath)
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
			if source != "_value" && source != "count * 1" {
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

	fmt.Println("koniec parse bucket script")

	// okay, we've checked everything, it's indeed a simple count
	return pipeline_aggregations.NewBucketScript(cw.Ctx), true
}

func (cw *ClickhouseQueryTranslator) parseBucketsPath(shouldBeQueryMap any, aggregationName string) (bucketsPathStr string, success bool) {
	queryMap, ok := shouldBeQueryMap.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("%s is not a map, but %T, value: %v", aggregationName, shouldBeQueryMap, shouldBeQueryMap)
		return
	}
	bucketsPathRaw, exists := queryMap["buckets_path"]
	if !exists {
		logger.WarnWithCtx(cw.Ctx).Msg("no buckets_path in avg_bucket")
		return
	}
	switch bucketsPath := bucketsPathRaw.(type) {
	case string:
		return bucketsPath, true
	case QueryMap:
		if len(bucketsPath) == 1 {
			for k, v := range bucketsPath {
				if k == "count" {
					return v.(string), true
				}
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("buckets_path is not a map with one key, but %d keys. Skipping this aggregation", len(bucketsPath))
		}
	}

	logger.WarnWithCtx(cw.Ctx).Msgf("buckets_path in wrong format, type: %T, value: %v", bucketsPathRaw, bucketsPathRaw)
	return
}
