// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package metrics_aggregations

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"math"
	"strconv"
	"strings"
	"time"
)

type Quantile struct {
	ctx             context.Context
	percentileNames []string // there may be multiple in one aggregation, it's a list of them in order of occurrence, e.g. ["25", "95"]
	keyed           bool     // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-aggregation.html#_keyed_response_6
	fieldType       database_common.DateTimeType
}

func NewQuantile(ctx context.Context, percentileNames []string, keyed bool, fieldType database_common.DateTimeType) Quantile {
	return Quantile{ctx, percentileNames, keyed, fieldType}
}

func (query Quantile) AggregationType() model.AggregationType {
	return model.MetricsAggregation
}

func (query Quantile) TranslateSqlResponseToJson(rows []model.QueryResultRow) model.JsonMap {
	valueMap := make(model.JsonMap)
	valueAsStringMap := make(model.JsonMap)

	if len(rows) == 0 {
		return query.emptyPercentilesResult()
	}
	if len(rows[0].Cols) == 0 {
		return query.emptyPercentilesResult()
	}

	percentileIdx := -1
	for _, res := range rows[0].Cols {
		if !strings.HasPrefix(res.ColName, "metric") {
			continue
		}

		// now we're sure we're dealing with a quantile
		percentileIdx++

		// error handling is moved to processResult
		percentile, percentileAsString, percentileIsNanOrInvalid := query.processResult(res.ColName, res.Value)

		percentileNameToReturn := query.createPercentileNameToReturn(query.percentileNames[percentileIdx])

		if percentileIsNanOrInvalid {
			valueMap[percentileNameToReturn] = nil
		} else {
			valueMap[percentileNameToReturn] = percentile
			if percentileAsString != nil {
				valueAsStringMap[percentileNameToReturn] = *percentileAsString
			}
		}
	}

	if query.keyed {
		return model.JsonMap{
			"values": valueMap,
		}
	} else {
		var values []model.JsonMap
		for _, percentileName := range query.percentileNames {
			key := query.createPercentileNameToReturn(percentileName)
			value := valueMap[key]
			keyAsFloat, _ := strconv.ParseFloat(key, 64)
			responseValue := model.JsonMap{
				"key":   keyAsFloat,
				"value": value,
			}
			if _, exists := valueAsStringMap[key]; exists {
				responseValue["value_as_string"] = valueAsStringMap[key]
			}
			values = append(values, responseValue)
		}
		return model.JsonMap{
			"values": values,
		}
	}
}

func (query Quantile) String() string {
	return fmt.Sprintf("quantile (keyed=%v, percentileNames=%v)", query.keyed, query.percentileNames)
}

// processResult processes the result of a single quantile value from Clickhouse, and handles all errors encountered.
// Unfortunately valueFromClickhouse is an array, even though we're only interested in [0] index.
// It makes this function a bit messy.
// That can be changed by changing the Clickhouse query, from `quantiles` to `quantile`, but it's well tested already + more general,
// I'd keep as it is for now, unless we find some further problems with it.
//
// Returns:
//   - percentile: float64 value of the percentile (or NaN if it's invalid)
//   - percentileAsString: string representation of the percentile
//     (or nil if we don't have it/don't need it - we'll just omit it in the response and that's fine)
//   - percentileIsNanOrInvalid: true if the percentile is NaN or invalid. We know we'll need to return nil in the response
func (query Quantile) processResult(colName string, percentileReturnedByClickhouse any) (
	percentile float64, percentileAsString *string, percentileIsNanOrInvalid bool) {
	var percentileAsArrayLen int
	// We never return from this switch preemptively to make code easier,
	// assumption is following: we know something is wrong if after the switch either
	// a) percentileAsArrayLen == 0, or b) percentileIsNanOrInvalid == true. Else => we're good.
	switch percentileTyped := percentileReturnedByClickhouse.(type) {
	case []float64:
		percentileAsArrayLen = len(percentileTyped)
		if len(percentileTyped) > 0 {
			percentileIsNanOrInvalid = math.IsNaN(percentileTyped[0])
			percentile = percentileTyped[0]
		}
	case float64:
		// The data read out in apache doris is float64
		percentileAsArrayLen = 1
		percentileIsNanOrInvalid = math.IsNaN(percentileTyped)
		percentile = percentileTyped
	case []time.Time:
		percentileAsArrayLen = len(percentileTyped)
		if len(percentileTyped) > 0 {
			percentile = float64(percentileTyped[0].UnixMilli())
			asString := percentileTyped[0].Format(time.RFC3339Nano)
			percentileAsString = &asString
		}
	case []any:
		percentileAsArrayLen = len(percentileTyped)
		if len(percentileTyped) > 0 {
			switch percentileTyped[0].(type) {
			case float64:
				return query.processResult(colName, []float64{percentileTyped[0].(float64)})
			case time.Time:
				return query.processResult(colName, []time.Time{percentileTyped[0].(time.Time)})
			default:
				logger.WarnWithCtx(query.ctx).Msgf("unexpected type in percentile array: %T, array: %v", percentileTyped[0], percentileTyped)
				percentileIsNanOrInvalid = true
			}
		}
	default:
		logger.WarnWithCtx(query.ctx).Msgf("unexpected type in percentile array: %T, value: %v", percentileReturnedByClickhouse, percentileReturnedByClickhouse)
		percentileIsNanOrInvalid = true
	}
	if percentileAsArrayLen == 0 {
		logger.WarnWithCtx(query.ctx).Msgf("empty percentile values for %s", colName)
		return math.NaN(), nil, true
	}
	if percentileIsNanOrInvalid {
		return math.NaN(), nil, true
	}
	return percentile, percentileAsString, percentileIsNanOrInvalid
}

func (query Quantile) emptyPercentilesResult() model.JsonMap {
	result := make(model.JsonMap, len(query.percentileNames))
	for _, percentileName := range query.percentileNames {
		result[query.createPercentileNameToReturn(percentileName)] = nil
	}
	return model.JsonMap{"values": result}
}

// Kibana requires .0 at the end of the percentile name if it's an integer.
func (query Quantile) createPercentileNameToReturn(percentileName string) string {
	// percentileName can't be an integer (doesn't work in Kibana that way), so we need to add .0 if it's missing
	dotIndex := strings.Index(percentileName, ".")
	if dotIndex == -1 {
		percentileName += ".0"
	}
	return percentileName
}

func (query Quantile) ColumnIdx(name string) int {
	for i, percentileName := range query.percentileNames {
		if percentileName == name {
			return i
		}
	}

	logger.ErrorWithCtx(query.ctx).Msgf("quantile column %s not found", name)
	return -1
}
