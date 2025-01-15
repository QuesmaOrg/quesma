// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func Test_removeDotsFromJsons(t *testing.T) {
	hostNameStr := `{"host.name": "alpha"}`
	hostNameJson, _ := types.ParseJSON(hostNameStr)

	fieldTransform := func(field string) string {
		return strings.ReplaceAll(field, ".", "::")
	}

	assert.True(t, transformFieldName(hostNameJson, fieldTransform))
	newBytes, _ := hostNameJson.Bytes()
	assert.Equal(t, `{"host::name":"alpha"}`, string(newBytes))

	nestedStr := `{"cloud": {"host.name":"alpha"}}`
	nestedJson, _ := types.ParseJSON(nestedStr)
	assert.True(t, transformFieldName(nestedJson, fieldTransform))
	newNested, _ := nestedJson.Bytes()
	assert.Equal(t, `{"cloud":{"host::name":"alpha"}}`, string(newNested))

	noChange := `{"host_name": "alpha"}`
	noChangeJson, _ := types.ParseJSON(noChange)
	assert.False(t, transformFieldName(noChangeJson, fieldTransform))
	newNoChange, _ := noChangeJson.Bytes()
	assert.Equal(t, `{"host_name":"alpha"}`, string(newNoChange))
}
