package queryparser

import "mitmproxy/quesma/logger"

func (cw *ClickhouseQueryTranslator) tryPipelineAggregation(queryMap QueryMap) string {
	return ""
}

func (cw *ClickhouseQueryTranslator) isItSimplePipelineCount(queryMap QueryMap) bool {
	bucketScriptRaw, exists := queryMap["bucket_script"]
	if !exists {
		return false
	}

	// so far we only handle "count" here :D
	delete(queryMap, "bucket_script")
	bucketScript, ok := bucketScriptRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("bucket_script is not a map, but %T, value: %v. Skipping this aggregation", bucketScriptRaw, bucketScriptRaw)
		return false
	}

	// if ["buckets_path"] != "_count", skip the aggregation
	if bucketsPathRaw, exists := bucketScript["buckets_path"]; exists {
		if bucketsPath, ok := bucketsPathRaw.(string); ok {
			if bucketsPath != "_count" {
				logger.WarnWithCtx(cw.Ctx).Msgf("buckets_path is not '_count', but %s. Skipping this aggregation", bucketsPath)
				return false
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("buckets_path is not a string, but %T, value: %v. Skipping this aggregation", bucketsPathRaw, bucketsPathRaw)
			return false
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msg("no buckets_path in bucket_script. Skipping this aggregation")
		return false
	}

	// if ["script"]["source"] != "_value", skip the aggregation
	scriptRaw, exists := bucketScript["script"]
	if !exists {
		logger.WarnWithCtx(cw.Ctx).Msg("no script in bucket_script. Skipping this aggregation")
		return false
	}
	script, ok := scriptRaw.(QueryMap)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("script is not a map, but %T, value: %v. Skipping this aggregation", scriptRaw, scriptRaw)
		return false
	}
	if sourceRaw, exists := script["source"]; exists {
		if source, ok := sourceRaw.(string); ok {
			if source != "_value" {
				logger.WarnWithCtx(cw.Ctx).Msgf("source is not '_value', but %s. Skipping this aggregation", source)
				return false
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("source is not a string, but %T, value: %v. Skipping this aggregation", sourceRaw, sourceRaw)
			return false
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msg("no source in script. Skipping this aggregation")
		return false
	}

	// okay, we've checked everything, it's indeed a simple count
	return true
}
