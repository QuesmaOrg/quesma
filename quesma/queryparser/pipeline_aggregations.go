// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/model/pipeline_aggregations"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"strings"
)

// CAUTION: maybe "return" everywhere isn't corrent, as maybe there can be multiple pipeline aggregations at one level.
// But I've tested some complex queries and it seems to not be the case. So let's keep it this way for now.
func (cw *ClickhouseQueryTranslator) parsePipelineAggregations(queryMap QueryMap) (aggregationType model.QueryType, err error) {
	parsers := map[string]aggregationParser{
		"bucket_script":  cw.parseBucketScriptBasic,
		"cumulative_sum": cw.parseCumulativeSum,
		"derivative":     cw.parseDerivative,
		"serial_diff":    cw.parseSerialDiff,
		"avg_bucket":     cw.parseAverageBucket,
		"min_bucket":     cw.parseMinBucket,
		"max_bucket":     cw.parseMaxBucket,
		"sum_bucket":     cw.parseSumBucket,
	}

	for aggrName, aggrParser := range parsers {
		if paramsRaw, exists := queryMap[aggrName]; exists {
			if params, ok := paramsRaw.(QueryMap); ok {
				delete(queryMap, aggrName)
				return aggrParser(params)
			}
			return nil, fmt.Errorf("%s is not a map, but %T, value: %v", aggrName, paramsRaw, paramsRaw)
		}
	}

	return nil, nil
}

func (cw *ClickhouseQueryTranslator) parseCumulativeSum(params QueryMap) (model.QueryType, error) {
	bucketsPath, err := cw.parseBucketsPath(params, "cumulative_sum")
	if err != nil {
		return nil, err
	}
	return pipeline_aggregations.NewCumulativeSum(cw.Ctx, bucketsPath), nil
}

func (cw *ClickhouseQueryTranslator) parseDerivative(params QueryMap) (model.QueryType, error) {
	bucketsPath, err := cw.parseBucketsPath(params, "derivative")
	if err != nil {
		return nil, err
	}
	return pipeline_aggregations.NewDerivative(cw.Ctx, bucketsPath), nil
}

func (cw *ClickhouseQueryTranslator) parseAverageBucket(params QueryMap) (model.QueryType, error) {
	bucketsPath, err := cw.parseBucketsPath(params, "avg_bucket")
	if err != nil {
		return nil, err
	}
	return pipeline_aggregations.NewAverageBucket(cw.Ctx, bucketsPath), nil
}

func (cw *ClickhouseQueryTranslator) parseMinBucket(params QueryMap) (model.QueryType, error) {
	bucketsPath, err := cw.parseBucketsPath(params, "min_bucket")
	if err != nil {
		return nil, err
	}
	return pipeline_aggregations.NewMinBucket(cw.Ctx, bucketsPath), nil
}

func (cw *ClickhouseQueryTranslator) parseMaxBucket(params QueryMap) (model.QueryType, error) {
	bucketsPath, err := cw.parseBucketsPath(params, "max_bucket")
	if err != nil {
		return nil, err
	}
	return pipeline_aggregations.NewMaxBucket(cw.Ctx, bucketsPath), nil
}

func (cw *ClickhouseQueryTranslator) parseSumBucket(params QueryMap) (model.QueryType, error) {
	bucketsPath, err := cw.parseBucketsPath(params, "sum_bucket")
	if err != nil {
		return nil, err
	}
	return pipeline_aggregations.NewSumBucket(cw.Ctx, bucketsPath), nil
}

func (cw *ClickhouseQueryTranslator) parseSerialDiff(params QueryMap) (model.QueryType, error) {
	// buckets_path
	bucketsPath, err := cw.parseBucketsPath(params, "serial_diff")
	if err != nil {
		return nil, err
	}

	// lag
	const defaultLag = 1
	lagRaw, exists := params["lag"]
	if !exists {
		return pipeline_aggregations.NewSerialDiff(cw.Ctx, bucketsPath, defaultLag), nil
	}
	if lag, ok := lagRaw.(float64); ok {
		return pipeline_aggregations.NewSerialDiff(cw.Ctx, bucketsPath, int(lag)), nil
	}

	return nil, fmt.Errorf("lag is not a float64, but %T, value: %v", lagRaw, lagRaw)
}

func (cw *ClickhouseQueryTranslator) parseBucketScriptBasic(params QueryMap) (model.QueryType, error) {
	bucketsPath, err := cw.parseBucketsPath(params, "bucket_script")
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(bucketsPath, pipeline_aggregations.BucketsPathCount) {
		//lint:ignore ST1005 I want Quesma capitalized
		return nil, fmt.Errorf("Quesma limitation, contact us if you need it fixed: buckets_path is not '_count', but %s", bucketsPath)
	}

	scriptRaw, exists := params["script"]
	if !exists {
		return nil, errors.New("no script in bucket_script")
	}
	if script, ok := scriptRaw.(string); ok {
		return pipeline_aggregations.NewBucketScript(cw.Ctx, bucketsPath, script), nil
	}

	script, ok := scriptRaw.(QueryMap)
	if !ok {
		return nil, fmt.Errorf("script is not a map, but %T, value: %v", scriptRaw, scriptRaw)
	}
	if sourceRaw, exists := script["source"]; exists {
		if source, ok := sourceRaw.(string); ok {
			if source != "_value" && source != "count * 1" {
				//lint:ignore ST1005 I want Quesma capitalized
				return nil, fmt.Errorf("Quesma limitation, contact us if you need it fixed: source is not '_value'/'count * 1', but %s", source)
			}
		} else {
			return nil, fmt.Errorf("source is not a string, but %T, value: %v", sourceRaw, sourceRaw)
		}
	} else {
		return nil, errors.New("no source in script")
	}

	// okay, we've checked everything, it's indeed a simple count
	return pipeline_aggregations.NewBucketScript(cw.Ctx, bucketsPath, ""), nil
}

func (cw *ClickhouseQueryTranslator) parseBucketsPath(params QueryMap, aggregationName string) (bucketsPathStr string, err error) {
	bucketsPathRaw, exists := params["buckets_path"]
	if !exists {
		return "", fmt.Errorf("no buckets_path in %s", aggregationName)
	}

	switch bucketsPath := bucketsPathRaw.(type) {
	case string:
		return bucketsPath, nil
	case QueryMap:
		// TODO: handle arbitrary nr of keys (and arbitrary scripts, because we also handle only one special case)
		if len(bucketsPath) == 1 || len(bucketsPath) == 2 {
			// We return just 1 value here (for smallest key) (determinism here important, returning any of them is incorrect)
			// Seems iffy, but works for all cases so far.
			// After fixing the TODO above, it should also get fixed.
			for _, key := range util.MapKeysSorted(bucketsPath) {
				if path, ok := bucketsPath[key].(string); ok {
					return path, nil
				} else {
					return "", fmt.Errorf("buckets_path is not a map with string values, but %T %v", bucketsPath[key], bucketsPath[key])
				}
			}
		} else {
			return "", fmt.Errorf("buckets_path is not a map with one or two keys, but it is: %v", bucketsPath)
		}
	}

	return "", fmt.Errorf("buckets_path in wrong format, type: %T, value: %v", bucketsPathRaw, bucketsPathRaw)
}
