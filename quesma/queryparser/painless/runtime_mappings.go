// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painless

import (
	"quesma/model"
	"quesma/quesma/types"
)

func ParseRuntimeMappings(body types.JSON) map[string]model.RuntimeMapping {

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
									mapping.Expr = ParsePainlessScriptToExpr(sourceAsString)
								}
							}
						}
					}
				}
				if mapping.Expr != nil {
					result[k] = mapping
				}
			}
		}
	}
	return result
}
