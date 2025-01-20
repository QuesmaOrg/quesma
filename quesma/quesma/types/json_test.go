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

func TestNDJSON_BulkForEach(t *testing.T) {

	input := `{"create":{"_index":"kibana_sample_data_flights", "_id": "1"}}
{"FlightNum":"9HY9SWR","DestCountry":"AU","OriginWeather":"Sunny","OriginCityName":"Frankfurt am Main" }
{"delete":{"_id":"task:Dashboard-dashboard_telemetry","_index":".kibana_task_manager_8.11.1"}}
{"create":{"_index":"kibana_sample_data_flights", "_id": "2"}}
{"FlightNum":"FOO","DestCountry":"BAR","OriginWeather":"BAZ","OriginCityName":"QUIX" }
`

	ndjson, err := ParseNDJSON(input)
	assert.NoError(t, err)

	err = ndjson.BulkForEach(func(entryNumber int, operationParsed BulkOperation, operation JSON, doc JSON) error {

		switch entryNumber {

		case 0:
			assert.Equal(t, "create", operationParsed.GetOperation())
			assert.Equal(t, "kibana_sample_data_flights", operationParsed.GetIndex())
			assert.Equal(t, "9HY9SWR", doc["FlightNum"])

		case 1:
			assert.Equal(t, "delete", operationParsed.GetOperation())
			assert.Equal(t, ".kibana_task_manager_8.11.1", operationParsed.GetIndex())

		case 2:
			assert.Equal(t, "create", operationParsed.GetOperation())
			assert.Equal(t, "kibana_sample_data_flights", operationParsed.GetIndex())
			assert.Equal(t, "FOO", doc["FlightNum"])

		default:
			t.Errorf("Unexpected entry number: %d", entryNumber)
		}

		return nil
	})

	assert.NoError(t, err)

}
