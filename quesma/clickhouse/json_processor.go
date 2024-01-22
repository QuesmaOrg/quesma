package clickhouse

import (
	"encoding/json"
	"fmt"
)

func preprocess(jsonStr string) string {
	var data map[string]interface{}
	_ = json.Unmarshal([]byte(jsonStr), &data)

	resultJSON, _ := json.Marshal(flattenMap(data))
	return string(resultJSON)
}

func flattenMap(data map[string]interface{}) map[string]interface{} {
	flattened := make(map[string]interface{})

	for key, value := range data {
		switch nested := value.(type) {
		case map[string]interface{}:
			nestedFlattened := flattenMap(nested)
			for nestedKey, nestedValue := range nestedFlattened {
				flattened[fmt.Sprintf("%s%s%s", key, nestedSeparator, nestedKey)] = nestedValue
			}
		default:
			flattened[key] = value
		}
	}

	return flattened
}
