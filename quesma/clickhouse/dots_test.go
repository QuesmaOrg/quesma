// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"quesma/quesma/types"
	"testing"
)

func Test_removeDotsFromJsons(t *testing.T) {
	hostNameStr := `{"host.name": "alpha"}`
	hostNameJson, _ := types.ParseJSON(hostNameStr)
	assert.True(t, replaceDotsWithSeparator(hostNameJson))
	newBytes, _ := hostNameJson.Bytes()
	assert.Equal(t, `{"host::name":"alpha"}`, string(newBytes))

	nestedStr := `{"cloud": {"host.name":"alpha"}}`
	nestedJson, _ := types.ParseJSON(nestedStr)
	assert.True(t, replaceDotsWithSeparator(nestedJson))
	newNested, _ := nestedJson.Bytes()
	assert.Equal(t, `{"cloud":{"host::name":"alpha"}}`, string(newNested))

	noChange := `{"host_name": "alpha"}`
	noChangeJson, _ := types.ParseJSON(noChange)
	assert.False(t, replaceDotsWithSeparator(noChangeJson))
	newNoChange, _ := noChangeJson.Bytes()
	assert.Equal(t, `{"host_name":"alpha"}`, string(newNoChange))
}
