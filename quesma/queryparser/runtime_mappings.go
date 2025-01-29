// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import (
	"github.com/QuesmaOrg/quesma/quesma/model"
	"github.com/QuesmaOrg/quesma/quesma/painful"
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
)

func ParseRuntimeMappings(body types.JSON) (map[string]model.RuntimeMapping, error) {

	result := make(map[string]model.RuntimeMapping)

	if runtimeMappings, ok := body["runtime_mappings"]; ok {
		if runtimeMappingsMap, ok := runtimeMappings.(map[string]interface{}); ok {
			for k, v := range runtimeMappingsMap {
				mapping := model.RuntimeMapping{
					Field: k,
				}
				if vAsMap, ok := v.(map[string]interface{}); ok {
					if typ, ok := vAsMap["type"]; ok {
						if typAsString, ok := typ.(string); ok {
							mapping.Type = typAsString
						}
					}
					if script, ok := vAsMap["script"]; ok {
						if scriptAsMap, ok := script.(map[string]interface{}); ok {
							if source, ok := scriptAsMap["source"]; ok {
								if sourceAsString, ok := source.(string); ok {

									dbExpr, postProcesExpr, err := ParseScript(sourceAsString)

									if err != nil {
										return nil, err
									}

									mapping.DatabaseExpression = dbExpr
									mapping.PostProcessExpression = postProcesExpr
								}
							}
						}
					}
				}
				if mapping.DatabaseExpression != nil {
					result[k] = mapping
				}
			}
		}
	}
	return result, nil
}

func ParseScript(s string) (model.Expr, painful.Expr, error) {

	// TODO: add a real parser here
	if s == "emit(doc['timestamp'].value.getHour());" {
		return model.NewFunction(model.DateHourFunction, model.NewColumnRef(model.TimestampFieldName)), nil, nil
	}

	expr, err := painful.ParsePainless(s)
	if err != nil {
		return nil, nil, err
	}

	// TODO here we can transform the parsed expression to an SQL

	// we return an empty SQL expression for given field, it'll make a column in the result set
	return model.NewLiteral("NULL"), expr, nil
}
