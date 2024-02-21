package util

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"reflect"
	"strconv"
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

	actualMinusExpected, expectedMinusActual := MapDifference(mActual, mExpected, false)
	assert.Equal(t, wantedActualMinusExpected, actualMinusExpected)
	assert.Equal(t, wantedExpectedMinusActual, expectedMinusActual)
}

func TestMapDifference_compareValues_different(t *testing.T) {
	mActual := JsonMap{"key": 101}
	mExpected := JsonMap{"key": 102}

	// if we don't compare values, maps are equal
	mdiff1, mdiff2 := MapDifference(mActual, mExpected, false)
	assert.Empty(t, mdiff1)
	assert.Empty(t, mdiff2)

	// if we compare values, maps are different
	mdiff1, mdiff2 = MapDifference(mActual, mExpected, true)
	assert.Equal(t, mActual, mdiff1)
	assert.Equal(t, mExpected, mdiff2)
}

func TestMapDifference_compareValues_floatEqualsInt(t *testing.T) {
	mActual := JsonMap{"key": 101}
	mExpected := JsonMap{"key": 101.00}
	mdiff1, mdiff2 := MapDifference(mActual, mExpected, true)
	assert.Empty(t, mdiff1)
	assert.Empty(t, mdiff2)
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
					"buckets": []any{
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

func TestMergeMaps(t *testing.T) {
	var cases = []struct {
		m1     JsonMap
		m2     JsonMap
		wanted JsonMap
	}{
		{
			JsonMap{"key1": "value1", "key2": "value2"},
			JsonMap{"key2": "value2", "key3": "value3"},
			JsonMap{"key1": "value1", "key2": "value2", "key3": "value3"},
		},
		{
			JsonMap{
				"start_time_in_millis": 0, "is_partial": false,
				"only-in-m1": "value", // different
				"response": JsonMap{
					"took": 0, "timed_out": false,
					"_shards": JsonMap{"total": 0, "successful": 0, "failed": 0, "skipped": 0},
					"hits":    JsonMap{"total": JsonMap{"value": 1, "relation": "eq"}, "max_score": 0, "hits": []any{}},
					"aggregations": JsonMap{"origins": JsonMap{"buckets": []any{
						JsonMap{ // different
							"distinations": JsonMap{
								"buckets": []any{
									JsonMap{
										"destLocation": JsonMap{
											"value": "New York",
										},
									},
								},
							},
						},
					}}},
				},
			},
			JsonMap{
				"start_time_in_millis": 0, "is_partial": false,
				"only-in-m2": "value", // different
				"response": JsonMap{
					"took": 0, "timed_out": false,
					"_shards": JsonMap{"total": 0, "successful": 0, "failed": 0, "skipped": 0},
					"hits":    JsonMap{"total": JsonMap{"value": 1, "relation": "eq"}, "max_score": 0, "hits": []any{}},
					"aggregations": JsonMap{"origins": JsonMap{"buckets": []any{
						JsonMap{ //different
							"distinations": JsonMap{
								"value": "New York",
							},
						},
					}}},
				},
			},
			JsonMap{
				"start_time_in_millis": 0, "is_partial": false,
				"only-in-m1": "value", "only-in-m2": "value", // merge from both maps
				"response": JsonMap{
					"took": 0, "timed_out": false,
					"_shards": JsonMap{"total": 0, "successful": 0, "failed": 0, "skipped": 0},
					"hits":    JsonMap{"total": JsonMap{"value": 1, "relation": "eq"}, "max_score": 0, "hits": []any{}},
					"aggregations": JsonMap{"origins": JsonMap{"buckets": []any{
						JsonMap{
							"distinations": JsonMap{
								"buckets": []any{ // from m1
									JsonMap{
										"destLocation": JsonMap{
											"value": "New York",
										},
									},
								},
								"value": "New York", // from m2
							},
						},
					}}},
				},
			},
		},
		{
			JsonMap{
				"origins": JsonMap{
					"buckets": []JsonMap{
						{
							"distinations": JsonMap{
								"buckets": []JsonMap{
									{
										"destLocation": JsonMap{
											"lat": "-34.8222",
											"lon": "-58.5358",
										},
									},
									{
										"destLocation": JsonMap{
											"lat": "-0.129166667",
											"lon": "-78.3575",
										},
									},
								},
							},
						},
						{
							"distinations": JsonMap{
								"buckets": []JsonMap{
									{
										"destLocation": JsonMap{
											"lat": "45.47060013",
											"lon": "-73.74079895",
										},
									},
									{
										"destLocation": JsonMap{
											"lat": "46.84209824",
											"lon": "-92.19360352",
										},
									},
								},
							},
						},
					},
				},
			},
			JsonMap{
				"origins": JsonMap{
					"buckets": []JsonMap{
						{
							"distinations": JsonMap{
								"buckets": []any{
									JsonMap{
										"DestAirportID": "EZE",
										"doc_count":     21,
										"key":           "EZE",
									},
									JsonMap{
										"doc_count":     12,
										"key":           "UI",
										"DestAirportID": "UIO",
									},
								},
							},
						},
						{
							"distinations": JsonMap{
								"buckets": []JsonMap{
									{
										"doc_count":     11,
										"key":           "YUL",
										"DestAirportID": "YUL",
									},
									{
										"DestAirportID": "EZE",
										"doc_count":     10,
										"key":           "EZE",
									},
								},
							},
						},
					},
				},
			},
			JsonMap{
				"origins": JsonMap{
					"buckets": []JsonMap{
						{
							"distinations": JsonMap{
								"buckets": []JsonMap{
									{
										"destLocation": JsonMap{
											"lat": "-34.8222",
											"lon": "-58.5358",
										},
										"DestAirportID": "EZE",
										"doc_count":     21,
										"key":           "EZE",
									},
									{
										"destLocation": JsonMap{
											"lat": "-0.129166667",
											"lon": "-78.3575",
										},
										"doc_count":     12,
										"key":           "UI",
										"DestAirportID": "UIO",
									},
								},
							},
						},
						{
							"distinations": JsonMap{
								"buckets": []JsonMap{
									{
										"destLocation": JsonMap{
											"lat": "45.47060013",
											"lon": "-73.74079895",
										},
										"doc_count":     11,
										"key":           "YUL",
										"DestAirportID": "YUL",
									},
									{
										"destLocation": JsonMap{
											"lat": "46.84209824",
											"lon": "-92.19360352",
										},
										"DestAirportID": "EZE",
										"doc_count":     10,
										"key":           "EZE",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	for i, tt := range cases {
		t.Run("TestMergeMaps_"+strconv.Itoa(i)+"m1,m2", func(t *testing.T) {
			if i == 2 {
				t.Skip("TODO. Will be fixed very soon, as it's needed for tests")
			}

			// simple == or Equal doesn't work on nested maps => need DeepEqual
			assert.True(t, reflect.DeepEqual(tt.wanted, MergeMaps(tt.m1, tt.m2)))
			// let's run again and swap m1 and m2. Behaviour might be different with bad implementation.
			assert.True(t, reflect.DeepEqual(tt.wanted, MergeMaps(tt.m2, tt.m1)))
		})
	}
}

func TestIsSqlEqual(t *testing.T) {
	var cases = []struct {
		sql1    string
		sql2    string
		isEqual bool
	}{
		{"abc", "ab", false},
		{"abc", "abc", true},
		{"abcd OR abc", "abc OR abcd", true},
		{"a OR (b AND c)", "a OR (c AND b)", true},
		{"a OR (b AND c)", "a OR (c OR b)", false},
		{
			`SELECT count() FROM add-this WHERE \"timestamp\"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z') AND \"timestamp\">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')`,
			`SELECT count() FROM add-this WHERE \"timestamp\">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND \"timestamp\"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')`,
			true,
		},
	}
	for i, tt := range cases {
		t.Run("TestIsSqlEqual_"+strconv.Itoa(i)+": "+tt.sql1+", "+tt.sql2, func(t *testing.T) {
			assert.Equal(t, tt.isEqual, IsSqlEqual(tt.sql1, tt.sql2))
		})
	}
}

func TestFilterNonEmpty(t *testing.T) {
	tests := []struct {
		array    []string
		filtered []string
	}{
		{
			[]string{"", ""},
			[]string{},
		},
		{
			[]string{"", "a", ""},
			[]string{"a"},
		},
		{
			[]string{"a", "b", "c", " ", "  "},
			[]string{"a", "b", "c", " ", "  "},
		},
	}
	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.Equal(t, tt.filtered, FilterNonEmpty(tt.array))
		})
	}
}

func Test_equal(t *testing.T) {
	tests := []struct {
		a, b any
		want bool
	}{
		{1, 1, true},
		{1, 2, false},
		{1, 1.5, false},
		{1.5, 1.5, true},
		{1, 1.0, true},
		{1.0, 1.0, true},
		{1.0, 1.0000000000000001, true},
		{1.0, 1, true},
	}
	for _, tt := range tests {
		got := equal(tt.a, tt.b)
		assert.Equal(t, tt.want, got)
	}
}
