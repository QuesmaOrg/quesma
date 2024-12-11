// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package bulk

import (
	"github.com/goccy/go-json"
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
