package queryparser

import (
	"fmt"
	"mitmproxy/quesma/model"
)

const maxPrecision = 0.999999

var defaultPercentiles = model.JsonMap{
	"1.0":  0.01,
	"5.0":  0.05,
	"25.0": 0.25,
	"50.0": 0.50,
	"75.0": 0.75,
	"95.0": 0.95,
	"99.0": 0.99,
}

const keyedDefaultValue = true

func (cw *ClickhouseQueryTranslator) parsePercentilesAggregation(queryMap QueryMap) (fieldName string, keyed bool, percentiles model.JsonMap) {
	if field, ok := queryMap["field"]; ok {
		fieldName = cw.Table.ResolveField(field.(string))
	}
	if keyedQueryMap, ok := queryMap["keyed"]; ok {
		keyed = keyedQueryMap.(bool)
	} else {
		keyed = keyedDefaultValue
	}

	if percents, ok := queryMap["percents"]; ok {
		userInput := percents.([]interface{})
		userSpecifiedPercents := make(model.JsonMap, len(userInput))
		for _, p := range userInput {
			asFloat := p.(float64)
			asString := fmt.Sprintf("%v", asFloat)
			asFloat = asFloat / 100
			if asFloat > maxPrecision {
				asFloat = maxPrecision // that's max precision used by Kibana UI and also the max we want to handle
			}
			userSpecifiedPercents[asString] = asFloat
		}
		return fieldName, keyed, userSpecifiedPercents
	} else {
		return fieldName, keyed, defaultPercentiles
	}
}
