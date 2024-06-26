// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package types

import "testing"

func TestParseRequestBody(t *testing.T) {

	ndjson := `{"create":{"_index":"device_logs"}}
{"client_id": "123"}
{"create":{"_index":"device_logs"}}
{"client_id": "234"}`

	// when
	responseBody := ParseRequestBody(ndjson)

	switch b := responseBody.(type) {
	case NDJSON:
		ndjsonData := b
		if len(ndjsonData) != 4 {
			t.Fatalf("Expected 4, got %v", len(ndjsonData))
		}
	default:
		t.Fatal("Invalid response body. Should be NDJSON")
	}
}

func TestParseRequestBody2(t *testing.T) {
	json := `{"client": "123"}`

	// when
	responseBody := ParseRequestBody(json)

	switch b := responseBody.(type) {
	case JSON:
		jsonData := b
		if jsonData["client"] != "123" {

			t.Fatalf("Expected 123, got %v", jsonData["client"])
		}
	default:
		t.Fatal("Invalid response body. Should be JSON")
	}

}
