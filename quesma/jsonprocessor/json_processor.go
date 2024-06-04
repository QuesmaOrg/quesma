package jsonprocessor

import (
	"fmt"
)

func FlattenMap(data map[string]interface{}, nestedSeparator string) map[string]interface{} {
	flattened := make(map[string]interface{})

	for key, value := range data {
		switch nested := value.(type) {
		case map[string]interface{}:
			nestedFlattened := FlattenMap(nested, nestedSeparator)
			for nestedKey, nestedValue := range nestedFlattened {
				flattened[fmt.Sprintf("%s%s%s", key, nestedSeparator, nestedKey)] = nestedValue
			}
		default:
			flattened[key] = value
		}
	}

	return flattened
}
