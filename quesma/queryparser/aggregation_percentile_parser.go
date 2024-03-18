package queryparser

import "fmt"

const maxPrecision = 0.999999

var DefaultPercentiles = map[string]float64{
	"1.0":  0.01,
	"5.0":  0.05,
	"25.0": 0.25,
	"50.0": 0.50,
	"75.0": 0.75,
	"95.0": 0.95,
	"99.0": 0.99,
}

func (cw *ClickhouseQueryTranslator) parsePercentilesAggregation(queryMap QueryMap) (string, map[string]float64) {
	var fieldName string
	if field, ok := queryMap["field"]; ok {
		fieldName = cw.Table.ResolveField(field.(string))
	}

	if percents, ok := queryMap["percents"]; ok {
		userInput := percents.([]interface{})
		userSpecifiedPercents := make(map[string]float64, len(userInput))
		for _, p := range userInput {
			asFloat := p.(float64)
			asString := fmt.Sprintf("%v", asFloat)
			asFloat = asFloat / 100
			if asFloat > maxPrecision {
				asFloat = maxPrecision // that's max precision used by Kibana UI and also the max we want to handle
			}
			userSpecifiedPercents[asString] = asFloat
		}
		return fieldName, userSpecifiedPercents
	} else {
		return fieldName, DefaultPercentiles
	}
}
