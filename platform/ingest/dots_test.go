// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
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

	assert.True(t, transformFieldName(hostNameJson, fieldTransform, fieldTransform))
	newBytes, _ := hostNameJson.Bytes()
	assert.Equal(t, `{"host::name":"alpha"}`, string(newBytes))

	nestedStr := `{"cloud": {"host.name":"alpha"}}`
	nestedJson, _ := types.ParseJSON(nestedStr)
	assert.True(t, transformFieldName(nestedJson, fieldTransform, fieldTransform))
	newNested, _ := nestedJson.Bytes()
	assert.Equal(t, `{"cloud":{"host::name":"alpha"}}`, string(newNested))

	noChange := `{"host_name": "alpha"}`
	noChangeJson, _ := types.ParseJSON(noChange)
	assert.False(t, transformFieldName(noChangeJson, fieldTransform, fieldTransform))
	newNoChange, _ := noChangeJson.Bytes()
	assert.Equal(t, `{"host_name":"alpha"}`, string(newNoChange))
}

func Test_removeDotsFromJsonsDigits(t *testing.T) {

	// test if we prefix the root level key with an underscore

	hostNameStr := `{"9": "2", "host.name": "alpha", "foo": {"10": "1"}}`

	jsonValue, err := types.ParseJSON(hostNameStr)
	if err != nil {
		t.Fatal(err)
	}

	res := transformFieldName(jsonValue, util.FieldToColumnEncoder, util.FieldPartToColumnEncoder)

	assert.True(t, res)

	foo, ok := jsonValue["foo"].(map[string]interface{})
	assert.True(t, ok)
	assert.NotNil(t, foo)

	assert.Equal(t, "2", jsonValue["_9"])
	assert.Equal(t, "1", foo["10"])
	assert.Equal(t, "alpha", jsonValue["host_name"])

}
