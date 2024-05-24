package util

import (
	"context"
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
				"buckets":                     []JsonMap{},
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
			"hits":      []JsonMap{},
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
			"hits": []any{
				JsonMap{
					"_index": "",
					"_id":    "",
					"_score": 0.000000,
					"_source": JsonMap{
						"COUNT()": 7.000000,
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
				"buckets":                     []JsonMap{},
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
					"COUNT()": 7.000000,
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

	actualMinusExpected, expectedMinusActual := MapDifference(mActual, mExpected, false, true)
	assert.Equal(t, wantedActualMinusExpected, actualMinusExpected)
	assert.Equal(t, wantedExpectedMinusActual, expectedMinusActual)
}

func TestMapDifference_arraysTypeDifference(t *testing.T) {
	mActual := JsonMap{
		"0": JsonMap{
			"buckets": []JsonMap{
				{
					"1": JsonMap{
						"buckets": []JsonMap{
							{"key_as_string": "2024-02-02T12:00:00.000+01:00", "doc_count": 2, "key": 1706871600000},
							{"key": 1706882400000, "key_as_string": "2024-02-02T15:00:00.000+01:00", "doc_count": 27},
							{"doc_count": 34, "key": 1706893200000, "key_as_string": "2024-02-02T18:00:00.000+01:00"},
						},
					},
				},
				{
					"1": JsonMap{
						"buckets": []JsonMap{
							{"doc_count": 0, "key": 1706871600000, "key_as_string": "2024-02-02T12:00:00.000+01:00"},
							{"doc_count": 2, "key": 1706882400000, "key_as_string": "2024-02-02T15:00:00.000+01:00"},
						},
					},
				},
			},
		},
	}
	mExpected := JsonMap{
		"0": JsonMap{
			"buckets": []any{
				JsonMap{
					"1": JsonMap{
						"buckets": []any{
							JsonMap{"key_as_string": "2024-02-02T12:00:00.000+01:00", "doc_count": 2.000000, "key": 1706871600000.000000},
							JsonMap{"key": 1706882400000.000000, "key_as_string": "2024-02-02T15:00:00.000+01:00", "doc_count": 27.000000},
							JsonMap{"doc_count": 34.000000, "key": 1706893200000.000000, "key_as_string": "2024-02-02T18:00:00.000+01:00"},
						},
					},
					"doc_count": 1647.000000, "key": "No Delay",
				},
				JsonMap{
					"key": "Security Delay",
					"1": JsonMap{
						"buckets": []any{
							JsonMap{"doc_count": 0.000000, "key": 1706871600000.000000, "key_as_string": "2024-02-02T12:00:00.000+01:00"},
							JsonMap{"doc_count": 2.000000, "key": 1706882400000.000000, "key_as_string": "2024-02-02T15:00:00.000+01:00"},
						},
					},
					"doc_count": 45.000000,
				},
			},
			"doc_count_error_upper_bound": 0.000000,
			"sum_other_doc_count":         0.000000,
		},
	}
	actualMinusExpected, expectedMinusActual := MapDifference(mActual, mExpected, true, true)
	assert.Empty(t, actualMinusExpected)
	assert.Equal(t, JsonMap{
		"0": JsonMap{
			"doc_count_error_upper_bound": 0.0, "sum_other_doc_count": 0.0,
			"buckets[0]": JsonMap{"doc_count": 1647.0, "key": "No Delay"},
			"buckets[1]": JsonMap{"doc_count": 45.0, "key": "Security Delay"},
		},
	}, expectedMinusActual)
}

func TestMapDifference_compareValues_different(t *testing.T) {
	mActual := JsonMap{"key": 101}
	mExpected := JsonMap{"key": 102}

	// if we don't compare values, maps are equal
	mdiff1, mdiff2 := MapDifference(mActual, mExpected, false, true)
	assert.Empty(t, mdiff1)
	assert.Empty(t, mdiff2)

	// if we compare values, maps are different
	mdiff1, mdiff2 = MapDifference(mActual, mExpected, true, true)
	assert.Equal(t, mActual, mdiff1)
	assert.Equal(t, mExpected, mdiff2)
}

func TestMapDifference_compareValues_floatEqualsInt(t *testing.T) {
	mActual := JsonMap{"key": 101}
	mExpected := JsonMap{"key": 101.00}
	mdiff1, mdiff2 := MapDifference(mActual, mExpected, true, true)
	assert.Empty(t, mdiff1)
	assert.Empty(t, mdiff2)
}

func TestMapDifference_compareFullArrays(t *testing.T) {
	var cases = []struct {
		actual                    JsonMap
		expected                  JsonMap
		wantedActualMinusExpected JsonMap
		wantedExpectedMinusActual JsonMap
	}{
		{
			JsonMap{
				"suggestions": JsonMap{
					"buckets": []JsonMap{
						{
							"key": "Rome", "doc_count": 73,
							"differentBase": "value1",
							"differentMap":  JsonMap{"nested-different": true},
						},
						{"key": "Bogota", "doc_count": 44},
						{"doc_count": 32, "key": "Milan"},
					},
				},
				"unique_terms": JsonMap{"value": 143},
			},
			JsonMap{
				"suggestions": JsonMap{
					"buckets": []interface{}{
						JsonMap{"doc_count": 73.000000, "key": "Rome"},
						JsonMap{"doc_count": 44.000000, "key": "Bogota"},
						JsonMap{"key": "Milan", "doc_count": 32.000000},
					},
					"doc_count_error_upper_bound": 0.000000,
					"sum_other_doc_count":         1706.000000,
				},
				"unique_terms": JsonMap{"value": 143.000000},
			},
			JsonMap{
				"suggestions": JsonMap{
					"buckets[0]": JsonMap{
						"differentBase": "value1",
						"differentMap":  JsonMap{"nested-different": true},
					},
				},
			},
			JsonMap{
				"suggestions": JsonMap{
					"doc_count_error_upper_bound": 0.000000,
					"sum_other_doc_count":         1706.000000,
				},
			},
		},
	}

	for _, tt := range cases {
		actualMinusExpected, expectedMinusActual := MapDifference(tt.actual, tt.expected, true, true)
		assert.True(t, reflect.DeepEqual(tt.wantedActualMinusExpected, actualMinusExpected))
		assert.True(t, reflect.DeepEqual(tt.wantedExpectedMinusActual, expectedMinusActual))
	}
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
					"hits":    JsonMap{"total": JsonMap{"value": 1, "relation": "eq"}, "max_score": 0, "hits": []JsonMap{}},
					"aggregations": JsonMap{"origins": JsonMap{"buckets": []JsonMap{
						JsonMap{ // different
							"distinations": JsonMap{
								"buckets": []JsonMap{
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
					"hits":    JsonMap{"total": JsonMap{"value": 1, "relation": "eq"}, "max_score": 0, "hits": []JsonMap{}},
					"aggregations": JsonMap{"origins": JsonMap{"buckets": []JsonMap{
						JsonMap{
							"distinations": JsonMap{
								"buckets": []JsonMap{ // from m1
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
		t.Run("TestMergeMaps_"+strconv.Itoa(i), func(t *testing.T) {
			// simple == or Equal doesn't work on nested maps => need DeepEqual
			assert.True(t, reflect.DeepEqual(tt.wanted, MergeMaps(context.Background(), tt.m1, tt.m2)))
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
			`SELECT COUNT() FROM add-this WHERE \"timestamp\"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z') AND \"timestamp\">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')`,
			`SELECT COUNT() FROM add-this WHERE \"timestamp\">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND \"timestamp\"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')`,
			true,
		},
		{
			`SELECT COUNT() FROM "logs-generic-default" WHERE ("FlightDelay" == true AND (("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z')) OR ("timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')))) AND ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			`SELECT COUNT() FROM "logs-generic-default" WHERE ("FlightDelay" == true AND (("timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z') AND "timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z')) OR ("timestamp"<=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp">=parseDateTime64BestEffort('2024-01-26T13:47:16.029Z')))) AND ("timestamp">=parseDateTime64BestEffort('2024-02-02T13:47:16.029Z') AND "timestamp"<=parseDateTime64BestEffort('2024-02-09T13:47:16.029Z'))`,
			true,
		},
	}
	for i, tt := range cases {
		t.Run("TestIsSqlEqual_"+strconv.Itoa(i)+": "+tt.sql1+", "+tt.sql2, func(t *testing.T) {
			assert.Equal(t, tt.isEqual, IsSqlEqual(tt.sql1, tt.sql2))
		})
	}
}

func TestAlmostEmpty(t *testing.T) {
	var cases = []struct {
		jsonMap              JsonMap
		acceptableDifference []string
		expectedResult       bool
	}{
		{
			JsonMap{"non-acceptable": true},
			[]string{},
			false,
		},
		{
			JsonMap{"acceptable": true},
			[]string{"acceptable"},
			true,
		},
		{
			JsonMap{"acceptable1": true, "doesnt-matter": JsonMap{"acceptable2": true}},
			[]string{"acceptable1", "acceptable2"},
			true,
		},
	}
	for i, tt := range cases {
		t.Run("TestAlmostEmpty_"+strconv.Itoa(i), func(t *testing.T) {
			assert.Equal(t, tt.expectedResult, AlmostEmpty(tt.jsonMap, tt.acceptableDifference))
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
		{int64(1), 1.0, true},
		{uint64(1), 1.0, true},
	}
	for _, tt := range tests {
		got := equal(tt.a, tt.b)
		assert.Equal(t, tt.want, got)
	}
}

func TestExtractInt64(t *testing.T) {
	i8 := int8(1)
	i16 := int16(1)
	i32 := int32(1)
	i64 := int64(1)
	ui8 := uint8(1)
	ui16 := uint16(1)
	ui32 := uint32(1)
	ui64 := uint64(1)
	tests := []struct {
		v    any
		want int64
	}{
		{int8(1), int64(1)},
		{int16(1), int64(1)},
		{int32(1), int64(1)},
		{int64(1), int64(1)},
		{uint8(1), int64(1)},
		{uint16(1), int64(1)},
		{uint32(1), int64(1)},
		{uint64(1), int64(1)},
		{&i8, int64(1)},
		{&i16, int64(1)},
		{&i32, int64(1)},
		{&i64, int64(1)},
		{&ui8, int64(1)},
		{&ui16, int64(1)},
		{&ui32, int64(1)},
		{&ui64, int64(1)},
		{nil, int64(-1)},
		{"1", int64(-1)},
		{1.0, int64(-1)},
	}
	for _, tt := range tests {
		got := ExtractInt64(tt.v)
		assert.Equal(t, tt.want, got)
	}
	for _, tt := range tests[:len(tests)-3] {
		got, success := ExtractInt64Maybe(tt.v)
		assert.Equal(t, tt.want, got)
		assert.True(t, success)
	}
	for _, tt := range []any{1.1, "1"} {
		_, success := ExtractInt64Maybe(tt)
		assert.False(t, success)
	}
}
