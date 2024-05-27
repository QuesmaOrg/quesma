package types

import (
	"encoding/json"
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

func TestReMarshalJSON(t *testing.T) {

	type dest struct {
		Key1 string `json:"key1"`
		Key2 string `json:"key2"`
	}

	// given
	jsonStr := `{"key1":"value1","key2":"value2"}`

	var jsonData JSON

	err := json.Unmarshal([]byte(jsonStr), &jsonData)
	if err != nil {
		t.Fatal(err)
	}

	// when
	var destData dest
	err = jsonData.Remarshal(&destData)
	if err != nil {
		t.Fatal(err)
	}

	// then

	assert.Equal(t, "value1", destData.Key1)
	assert.Equal(t, "value2", destData.Key2)

}
