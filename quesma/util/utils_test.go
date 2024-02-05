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

func TestMapDifference(t *testing.T) {
	mActual := JsonMap{
		"aggregations": JsonMap{
			"suggestions": JsonMap{
				"buckets":                     []interface{}{},
				"doc_count_error_upper_bound": 0.000000,
				"sum_other_doc_count":         0.000000,
			},
			"unique_terms": JsonMap{
				"value": 0.000000,
			},
		},
		"took":             2.000000,
		"timed_out":        false,
		"terminated_early": false,
		"_shards": JsonMap{
			"successful": 2.000000,
			"skipped":    0.000000,
			"failed":     0.000000,
			"total":      2.000000,
		},
		"hits": JsonMap{
			"max_score": nil,
			"hits":      []interface{}{},
			"total": JsonMap{
				"value":    6.000000,
				"relation": "eq",
			},
		},
	}

	mExpected := JsonMap{
		"took":      0.000000,
		"timed_out": false,
		"_shards": JsonMap{
			"total":      0.000000,
			"successful": 0.000000,
			"failed":     0.000000,
			"failures":   nil,
			"skipped":    0.000000,
		},
		"hits": JsonMap{
			"max_score": 0.000000,
			"hits": []interface{}{
				JsonMap{
					"_index": "",
					"_id":    "",
					"_score": 0.000000,
					"_source": JsonMap{
						"count()": 7.000000,
					},
					"_type": "",
					"sort":  nil,
				},
			},
			"total": JsonMap{
				"value":    0.000000,
				"relation": "",
			},
		},
		"errors":       false,
		"aggregations": nil,
	}

	wantedActualMinusExpected := JsonMap{
		"aggregations": JsonMap{
			"suggestions": JsonMap{
				"buckets":                     []interface{}{},
				"doc_count_error_upper_bound": 0.000000,
				"sum_other_doc_count":         0.000000,
			},
			"unique_terms": JsonMap{
				"value": 0.000000,
			},
		},
		"terminated_early": false,
	}

	wantedExpectedMinusActual := JsonMap{
		"hits": JsonMap{
			"hits[0]": JsonMap{
				"_id":    "",
				"_index": "",
				"_score": 0.000000,
				"_source": JsonMap{
					"count()": 7.000000,
				},
				"_type": "",
				"sort":  nil,
			},
		},
		"errors": false,
		"_shards": JsonMap{
			"failures": nil,
		},
	}

	actualMinusExpected, expectedMinusActual := MapDifference(mActual, mExpected)
	assert.Equal(t, wantedActualMinusExpected, actualMinusExpected)
	assert.Equal(t, wantedExpectedMinusActual, expectedMinusActual)
}

// regression test, it used to fail before fix.
func TestJsonDifference(t *testing.T) {
	actual := `
	{
		"start_time_in_millis": 0,
		"completion_time_in_millis": 0,
		"expiration_time_in_millis": 0,
		"id": "fake-id",
		"is_running": false,
		"is_partial": false,
		"response": {
			"took": 0,
			"timed_out": false,
			"_shards": {
				"total": 0,
				"successful": 0,
				"failed": 0,
				"skipped": 0
			},
			"hits": {
				"total": {
					"value": 0,
					"relation": ""
				},
				"max_score": 0,
				"hits": null
			}
		}
	}`
	expected := `
	{
		"completion_time_in_millis": 1706639337527,
		"expiration_time_in_millis": 1706639397521,
		"id": "FnhMY09KX3ZLUmFDeGtjLU1YM1RMMGccTTF2dnY2R0dSNEtZYVQ3cjR5ZnBuQTo3NjM0MQ==",
		"is_partial": false,
		"is_running": false,
		"response": {
			"_shards": {
				"failed": 0,
				"skipped": 0,
				"successful": 1,
				"total": 1
			},
			"aggregations": {
				"0": {
					"buckets": [
						{
							"doc_count": 1,
							"key": 1706638410000,
							"key_as_string": "2024-01-30T19:13:30.000+01:00"
						},
						{
							"doc_count": 14,
							"key": 1706638440000,
							"key_as_string": "2024-01-30T19:14:00.000+01:00"
						}
					]
				}
			},
			"hits": {
				"hits": [],
				"max_score": null,
				"total": {
					"relation": "eq",
					"value": 87
				}
			},
			"timed_out": false,
			"took": 6
		},
		"start_time_in_millis": 1706639337521
	}`
	wantedExpectedMinusActual := JsonMap{
		"response": JsonMap{
			"aggregations": JsonMap{
				"0": JsonMap{
					"buckets": []interface{}{
						JsonMap{
							"doc_count":     1.0,
							"key":           1706638410000.0,
							"key_as_string": "2024-01-30T19:13:30.000+01:00",
						},
						JsonMap{
							"doc_count":     14.0,
							"key":           1706638440000.0,
							"key_as_string": "2024-01-30T19:14:00.000+01:00",
						},
					},
				},
			},
		},
	}

	actualMinusExpected, expectedMinusActual, err := JsonDifference(actual, expected)
	assert.NoError(t, err)
	assert.Empty(t, actualMinusExpected)
	assert.Equal(t, wantedExpectedMinusActual, expectedMinusActual)
}
