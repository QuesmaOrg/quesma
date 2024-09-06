// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"quesma/quesma/types"
	"strings"
)

func replaceDotsWithSeparator(jsonInsert types.JSON) bool {
	gotDots := false
	for fieldName, v := range jsonInsert {
		withoutDotsFieldName := strings.Replace(fieldName, ".", ".", -1)
		if fieldName != withoutDotsFieldName {
			gotDots = true
			jsonInsert[withoutDotsFieldName] = v
			delete(jsonInsert, fieldName)
			fieldName = withoutDotsFieldName
		}
		if nestedJson, isNested := v.(map[string]interface{}); isNested {
			nestedGotDots := replaceDotsWithSeparator(nestedJson)
			gotDots = gotDots || nestedGotDots
		}
	}
	return gotDots
}
