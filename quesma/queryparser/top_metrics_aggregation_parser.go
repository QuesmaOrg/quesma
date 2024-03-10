package queryparser

func ParseTopMetricsAggregation(queryMap QueryMap) metricsAggregation {
	var fieldList []interface{}
	if fields, ok := queryMap["metrics"].([]interface{}); ok {
		fieldList = fields
	} else {
		fieldList = append(fieldList, queryMap["metrics"])
	}
	fieldNames := getFieldNames(fieldList)
	sortBy, order := getFirstKeyValue(queryMap["sort"].(QueryMap))

	var size int
	if _, ok := queryMap["size"]; ok {
		size = int(queryMap["size"].(float64))
	} else {
		size = 1
	}
	return metricsAggregation{
		AggrType:   "top_metrics",
		FieldNames: fieldNames,
		SortBy:     sortBy,
		Size:       size,
		Order:      order,
	}
}

func getFirstKeyValue(queryMap QueryMap) (string, string) {
	for k, v := range queryMap {
		return k, v.(string)
	}
	return "", ""
}

func getFieldNames(fields []interface{}) []string {
	var fieldNames []string
	for _, field := range fields {
		if fName, ok := field.(QueryMap)["field"]; ok {
			fieldNames = append(fieldNames, fName.(string))
		}
	}
	return fieldNames
}
