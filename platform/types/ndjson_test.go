// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package types

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseNDJSON(t *testing.T) {

	ndjson := `{"create":{"_index":"device_logs"}}
{"client_id": "123"}
{"create":{"_index":"device_logs"}}
{"client_id": "234"}`

	// when
	responseBody := ParseRequestBody(ndjson)

	switch b := responseBody.(type) {
	case NDJSON:
		ndjsonData := b
		assert.Equal(t, 4, len(ndjsonData))
	default:
		t.Fatal("Invalid response body. Should be NDJSON")
	}

}
