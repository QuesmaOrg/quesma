// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"context"
	"quesma/model"
	"quesma/util"
)

func pancakeRenderJSONLayer(layers []*pancakeAggregationLayer, rows []model.QueryResultRow) model.JsonMap {
	result := model.JsonMap{}
	if len(layers) == 0 {
		return result
	}
	layer := layers[0]
	for _, metric := range layer.currentMetricAggregations {
		result[metric.name] = model.JsonMap{
			"aggregation": "metric",
		}
	}

	if layer.nextBucketAggregation != nil {
		bucket := model.JsonMap{
			"aggregation": "bucket",
		}
		if len(layers) > 1 {
			// TODO: context
			subAggr := pancakeRenderJSONLayer(layers[1:], rows)
			bucket = util.MergeMaps(context.Background(), bucket, subAggr, model.KeyAddedByQuesma)
		}

		result[layer.nextBucketAggregation.name] = model.JsonMap{
			"buckets": []model.JsonMap{
				bucket,
			},
		}
	}
	return result
}

func pancakeRenderJSON(agg *pancakeAggregation, rows []model.QueryResultRow) model.JsonMap {
	return pancakeRenderJSONLayer(agg.layers, rows)
}
