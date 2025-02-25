// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"github.com/QuesmaOrg/quesma/quesma/types"
)

func transformFieldName(jsonInsert types.JSON, transformer func(field string) string) bool {
	gotDots := false
	for fieldName, v := range jsonInsert {
		withoutDotsFieldName := transformer(fieldName)
		if fieldName != withoutDotsFieldName {
			gotDots = true
			jsonInsert[withoutDotsFieldName] = v
			delete(jsonInsert, fieldName)
			fieldName = withoutDotsFieldName
		}
		if nestedJson, isNested := v.(map[string]interface{}); isNested {
			nestedGotDots := transformFieldName(nestedJson, transformer)
			gotDots = gotDots || nestedGotDots
		}
	}
	return gotDots
}
