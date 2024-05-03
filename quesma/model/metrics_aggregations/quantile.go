package metrics_aggregations

import (
	"context"
	"math"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"strconv"
	"strings"
)

type Quantile struct {
	ctx   context.Context
	keyed bool // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-aggregation.html#_keyed_response_6
}

func NewQuantile(ctx context.Context, keyed bool) Quantile {
	return Quantile{ctx, keyed}
}

func (query Quantile) IsBucketAggregation() bool {
	return false
}

func (query Quantile) TranslateSqlResponseToJson(rows []model.QueryResultRow, level int) []model.JsonMap {
	valueMap := make(model.JsonMap)

	if len(rows) == 0 {
		return emptyPercentilesResult
	}
	if len(rows[0].Cols) == 0 {
		return emptyPercentilesResult
	}

	for _, res := range rows[0].Cols {
		if strings.HasPrefix(res.ColName, "quantile") {
			percentile, ok := res.Value.([]float64)
			if !ok {
				logger.WarnWithCtx(query.ctx).Msgf(
					"failed to convert percentile values to []float64, type: %T, value: %v. Skipping", res.Value, res.Value)
				continue
			}
			percentileName, _ := strings.CutPrefix(res.ColName, "quantile_")

			// percentileName can't be an integer (doesn't work in Kibana that way), so we need to add .0 if it's missing
			dotIndex := strings.Index(percentileName, ".")
			if dotIndex == -1 {
				percentileName += ".0"
			}

			if len(percentile) == 0 {
				logger.WarnWithCtx(query.ctx).Msgf("empty percentile values for %s", percentileName)
			}
			if len(percentile) == 0 || math.IsNaN(percentile[0]) {
				valueMap[percentileName] = nil
			} else {
				valueMap[percentileName] = percentile[0]
			}
		}
	}

	if query.keyed {
		return []model.JsonMap{{
			"values": valueMap,
		}}
	} else {
		var values []model.JsonMap
		for key, value := range valueMap {
			keyAsFloat, _ := strconv.ParseFloat(key, 64)
			values = append(values, model.JsonMap{
				"key":   keyAsFloat,
				"value": value,
			})
		}
		return []model.JsonMap{{
			"values": values,
		}}
	}
}

func (query Quantile) String() string {
	return "quantile"
}

var emptyPercentilesResult = []model.JsonMap{{
	"values": 0,
}}
