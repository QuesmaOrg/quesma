// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package jsonprocessor

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
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

type RewriteArrayOfObject struct{}

func (t *RewriteArrayOfObject) rewrite(array []interface{}) (map[string]interface{}, error) {

	fields := make(map[string]interface{})
	for i, el := range array {
		if obj, ok := el.(map[string]interface{}); ok {
			for key := range obj {
				fields[key] = true
			}
		} else {
			return nil, fmt.Errorf("element %d of array is not an object", i)
		}
	}

	result := make(map[string]interface{})

	for field := range fields {
		var fieldValue []interface{}

		for i, element := range array {
			if obj, ok := element.(map[string]interface{}); ok {
				if _, ok := obj[field]; ok {
					fieldValue = append(fieldValue, obj[field])
				} else {
					fieldValue = append(fieldValue, nil)
				}
			} else {
				return nil, fmt.Errorf("element %d of array is not an object", i)
			}
		}
		result[field] = fieldValue
	}

	return result, nil
}

// RewriteArrayOfObject rewrites an array of objects into map of arrays
func (t *RewriteArrayOfObject) Transform(data types.JSON) (types.JSON, error) {

	for k, v := range data {
		switch val := v.(type) {

		case types.JSON:

			res, err := t.Transform(val)

			if err != nil {
				return nil, err
			}
			data[k] = res

		case []interface{}:

			if len(val) == 0 {
				continue
			}

			first := val[0]
			if _, ok := first.(map[string]interface{}); ok {
				v, err := t.rewrite(val)
				if err != nil {
					return nil, err
				}
				data[k] = v

			} else {
				for i, item := range val {
					switch itemVal := item.(type) {
					case map[string]interface{}:
						res, err := t.Transform(itemVal)
						if err != nil {
							return nil, err
						}
						data[k].([]interface{})[i] = res
					}
				}
			}
		}
	}

	return data, nil
}

type RemoveFieldsOfObject struct {
	RemovedFields []config.FieldName
}

func (t *RemoveFieldsOfObject) Transform(data types.JSON) (types.JSON, error) {
	for _, field := range t.RemovedFields {
		delete(data, field.AsString())
	}
	return data, nil
}
