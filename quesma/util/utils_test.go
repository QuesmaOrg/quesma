package util

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJsonPrettifyShortenArrays(t *testing.T) {
	str := `{
		"person": {
			"name": "Alice",
			"age": 25,
			"address": {
				"city": "Wonderland",
				"zip": "12345"
			},
			"friends": [
				{"name": "Bob", "age": 28},
				{"name": "Charlie", "age": 30},
				{"name": "David", "age": 27},
				{"name": "Eve", "age": 26},
				{"name": "Frank", "age": 29},
				{"name": "Grace", "age": 31},
				{"name": "Henry", "age": 32},
				{"name": "Ivy", "age": 33}
			]
		}
	}`

	// when
	str = JsonPrettify(str, true)

	// then
	result := make(map[string]interface{})
	_ = json.Unmarshal([]byte(str), &result)

	person := result["person"].(map[string]interface{})
	friends := person["friends"].([]interface{})
	assert.Equal(t, 3, len(friends))
	assert.Equal(t, "...", friends[2])
}
