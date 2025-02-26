// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"github.com/QuesmaOrg/quesma/quesma/types"
)

type FieldTransformer func(field string) string

func transformFieldName(jsonInsert types.JSON, transformer FieldTransformer, nestedTransformer FieldTransformer) bool {
	return transformFieldNameInternal(0, jsonInsert, transformer, nestedTransformer)
}

func transformFieldNameInternal(level int, jsonInsert types.JSON, transformer FieldTransformer, nestedTransformer FieldTransformer) bool {
	gotDots := false
	for fieldName, v := range jsonInsert {
		var withoutDotsFieldName string
		if level == 0 {
			withoutDotsFieldName = transformer(fieldName)
		} else {
			withoutDotsFieldName = nestedTransformer(fieldName)
		}
		if fieldName != withoutDotsFieldName {
			gotDots = true
			jsonInsert[withoutDotsFieldName] = v
			delete(jsonInsert, fieldName)
			fieldName = withoutDotsFieldName
		}
		if nestedJson, isNested := v.(map[string]interface{}); isNested {
			nestedGotDots := transformFieldNameInternal(level+1, nestedJson, transformer, nestedTransformer)
			gotDots = gotDots || nestedGotDots
		}
	}
	return gotDots
}
