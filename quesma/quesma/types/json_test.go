// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package types

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCommentedJson(t *testing.T) {
	jsonStr := `{"key1":"value1","key2":"value2"}`
	commentedJsonStr := `// comment
{"key1":"value1","key2":"value2" /* another comment */ }`

	jsonStruct, err := ParseJSON(commentedJsonStr)
	assert.NoError(t, err)
	withoutComment := jsonStruct.ShortString()

	assert.Equal(t, jsonStr, withoutComment)
}

func TestJSONClone(t *testing.T) {
	simpleJson := JSON{"key1": "value1", "key2": "value2"}
	clonedA := simpleJson.Clone()
	delete(simpleJson, "key1")
	assert.Equal(t, "value1", clonedA["key1"])

	deepJson := JSON{"key1": "value1", "key2": JSON{"key3": "value3"}}
	clonedB := deepJson.Clone()
	delete(deepJson["key2"].(JSON), "key3")
	assert.Equal(t, "value3", clonedB["key2"].(JSON)["key3"])

	arrayJson := JSON{"key1": "value1", "key2": []JSON{JSON{"key3": "value3"}, JSON{"key4": "value4"}}}
	clonedC := arrayJson.Clone()
	delete(arrayJson["key2"].([]JSON)[0], "key3")
	assert.Equal(t, "value3", clonedC["key2"].([]JSON)[0]["key3"])
}
