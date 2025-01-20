// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bulk

import (
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_unmarshalElasticResponse(t *testing.T) {
	tests := []struct {
		name                    string
		bulkResponseFromElastic string
	}{
		{
			name:                    "bulk response with no errors (1)",
			bulkResponseFromElastic: `{"errors":false,"took":12,"items":[{"create":{"_index":"testcase15","_id":"XLkV5JABtqi1BREg-Ldw","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":7,"_primary_term":1,"status":201}}]}`,
		},
		{
			name:                    "bulk response with no errors (2)",
			bulkResponseFromElastic: `{"errors":false,"took":68,"items":[{"create":{"_index":"testcase15","_id":"XrkW5JABtqi1BREgWbeP","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":9,"_primary_term":1,"status":201}}]}`,
		},
		{
			name:                    "bulk response with some error",
			bulkResponseFromElastic: `{"errors":true,"took":28,"items":[{"create":{"_index":"testcase15","_id":"X7kW5JABtqi1BREgc7eg","status":400,"error":{"type":"document_parsing_exception","reason":"[1:14] failed to parse field [newcolumn] of type [long] in document with id 'X7kW5JABtqi1BREgc7eg'. Preview of field's value: 'invalid'","caused_by":{"type":"illegal_argument_exception","reason":"For input string: \"invalid\""}}}}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bulkResponse := &BulkResponse{}
			if err := json.Unmarshal([]byte(tt.bulkResponseFromElastic), bulkResponse); err != nil {
				t.Errorf("error while unmarshaling elastic response: %v", err)
			}

			marshaled, err := json.Marshal(bulkResponse)
			if err != nil {
				t.Errorf("error while marshaling elastic response: %v", err)
			}

			require.JSONEq(t, tt.bulkResponseFromElastic, string(marshaled), "unmarshaled and marshaled response should be the same")
		})
	}
}

func Test_BulkForEach(t *testing.T) {
	input := `{"create":{"_index":"kibana_sample_data_flights", "_id": "1"}}
{"FlightNum":"9HY9SWR","DestCountry":"AU","OriginWeather":"Sunny","OriginCityName":"Frankfurt am Main" }
{"delete":{"_id":"task:Dashboard-dashboard_telemetry","_index":".kibana_task_manager_8.11.1"}}
{"delete":{"_id":"task:Dashboard-dashboard_telemetry","_index":".kibana_task_manager"}}
{"delete":{"_id":"task:Dashboard-dashboard_telemetry","_index":".kibana_task_manager_X"}}
{"create":{"_index":"kibana_sample_data_flights", "_id": "2"}}
{"FlightNum":"FOO","DestCountry":"BAR","OriginWeather":"BAZ","OriginCityName":"QUIX" }
`
	ndjson, err := types.ParseNDJSON(input)
	assert.NoError(t, err)

	err = ndjson.BulkForEach(func(entryNumber int, operationParsed types.BulkOperation, operation types.JSON, doc types.JSON) error {
		switch entryNumber {
		case 0:
			assert.Equal(t, "create", operationParsed.GetOperation())
			assert.Equal(t, "kibana_sample_data_flights", operationParsed.GetIndex())
			assert.Equal(t, "9HY9SWR", doc["FlightNum"])

		case 1:
			assert.Equal(t, "delete", operationParsed.GetOperation())
			assert.Equal(t, ".kibana_task_manager_8.11.1", operationParsed.GetIndex())

		case 2:
			assert.Equal(t, "delete", operationParsed.GetOperation())
			assert.Equal(t, ".kibana_task_manager", operationParsed.GetIndex())

		case 3:
			assert.Equal(t, "delete", operationParsed.GetOperation())
			assert.Equal(t, ".kibana_task_manager_X", operationParsed.GetIndex())

		case 4:
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

func Test_BulkForEachDeleteOnly(t *testing.T) {
	input := `{"delete":{"_id":"task:Dashboard-dashboard_telemetry","_index":".kibana_task_manager_8.11.1"}}`
	ndjson, err := types.ParseNDJSON(input)
	assert.NoError(t, err)

	err = ndjson.BulkForEach(func(entryNumber int, operationParsed types.BulkOperation, operation types.JSON, doc types.JSON) error {
		switch entryNumber {
		case 0:
			assert.Equal(t, "delete", operationParsed.GetOperation())
			assert.Equal(t, ".kibana_task_manager_8.11.1", operationParsed.GetIndex())
		default:
			t.Errorf("Unexpected entry number: %d", entryNumber)
		}

		return nil
	})
	assert.NoError(t, err)
}
