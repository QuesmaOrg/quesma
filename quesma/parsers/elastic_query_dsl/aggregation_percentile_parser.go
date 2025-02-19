// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elastic_query_dsl

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/model"
)

const maxPrecision = 0.999999

var defaultPercentiles = map[string]float64{
	"1.0":  0.01,
	"5.0":  0.05,
	"25.0": 0.25,
	"50.0": 0.50,
	"75.0": 0.75,
	"95.0": 0.95,
	"99.0": 0.99,
}

const keyedDefaultValue = true

func (cw *ClickhouseQueryTranslator) parsePercentilesAggregation(queryMap QueryMap) (field model.Expr, keyed bool, percentiles map[string]float64) {
	field = cw.parseFieldField(queryMap, "percentile")
	if keyedQueryMap, ok := queryMap["keyed"]; ok {
		if keyed, ok = keyedQueryMap.(bool); !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("keyed specified for percentiles aggregation is not a boolean. Querymap: %v", queryMap)
			keyed = keyedDefaultValue
		}
	} else {
		keyed = keyedDefaultValue
	}

	percents, ok := queryMap["percents"]
	if !ok {
		return field, keyed, defaultPercentiles
	}
	userInput, ok := percents.([]interface{})
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("percents specified for percentiles aggregation is not an array. Querymap: %v", queryMap)
		return field, keyed, defaultPercentiles
	}
	userSpecifiedPercents := make(map[string]float64, len(userInput))
	for _, p := range userInput {
		asFloat, ok := p.(float64)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("percent specified for percentiles aggregation is not a float. Skipping. Querymap: %v", queryMap)
			continue
		}
		asString := fmt.Sprintf("%v", asFloat)
		asFloat = asFloat / 100
		if asFloat > maxPrecision {
			asFloat = maxPrecision // that's max precision used by Kibana UI and also the max we want to handle
		}
		userSpecifiedPercents[asString] = asFloat
	}
	return field, keyed, userSpecifiedPercents
}
