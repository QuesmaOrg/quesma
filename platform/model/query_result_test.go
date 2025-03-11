// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestQueryResultCol_String(t *testing.T) {
	var str = ""
	var strPtr, strPtrNil *string = &str, nil

	var Time = time.Time{}
	var timePtr, timePtrNil *time.Time = &Time, nil

	var Int64 = int64(1)
	var int64Ptr, int64PtrNil *int64 = &Int64, nil

	var Float64 = float64(1)
	var float64Ptr, float64PtrNil *float64 = &Float64, nil

	var Int = 1
	var intPtr, intPtrNil *int = &Int, nil

	var Bool = true
	var boolPtr, boolPtrNil *bool = &Bool, nil

	var testcases = []struct {
		value    any
		expected string
	}{
		{"test", `"name": "test"`},
		{`test "GET"`, `"name": "test \"GET\""`},
		{1, `"name": 1`},
		{1.0, `"name": 1`},
		{int64(1), `"name": 1`},
		{uint64(1), `"name": 1`},
		{true, `"name": true`},
		{time.Time{}, `"name": "0001-01-01 00:00:00 +0000 UTC"`},
		{strPtr, `"name": ""`},
		{strPtrNil, ``},
		{timePtr, `"name": "0001-01-01 00:00:00 +0000 UTC"`},
		{timePtrNil, ``},
		{int64Ptr, `"name": 1`},
		{int64PtrNil, ``},
		{float64Ptr, `"name": 1`},
		{float64PtrNil, ``},
		{intPtr, `"name": 1`},
		{intPtrNil, ``},
		{boolPtr, `"name": true`},
		{boolPtrNil, ``},
		{[]string{"a", "b"}, `"name": ["a","b"]`},
		{[]int{1, 2}, `"name": [1,2]`},
		{[]int64{1, 2}, `"name": [1,2]`},
		{[]float64{1, 2}, `"name": [1,2]`},
		{[]bool{true, false}, `"name": [true,false]`},
		{map[string]string{"a": "b"}, `"name": {"a":"b"}`},
		{map[string]any{"a": 1}, `"name": {"a":1}`},
		{map[string]any{"a": map[string]any{"int": 1}}, `"name": {"a":{"int":1}}`},
	}
	ctx := context.Background()
	for _, tt := range testcases {
		t.Run(tt.expected, func(t *testing.T) {
			col := QueryResultCol{ColName: "name", Value: tt.value}
			got := col.String(ctx)
			if got != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestQueryResultCol_ExtractValue(t *testing.T) {
	var str = ""
	var strPtr, strPtrNil *string = &str, nil

	var Time = time.Time{}
	var timePtr, timePtrNil *time.Time = &Time, nil

	var Int64 = int64(1)
	var int64Ptr, int64PtrNil *int64 = &Int64, nil

	var Float64 = float64(1)
	var float64Ptr, float64PtrNil *float64 = &Float64, nil

	var Int = 1
	var intPtr, intPtrNil *int = &Int, nil

	var Bool = true
	var boolPtr, boolPtrNil *bool = &Bool, nil

	var testcases = []struct {
		value    any
		expected any
	}{
		{str, ""},
		{1, 1},
		{1.0, 1.0},
		{int64(1), int64(1)},
		{uint64(1), uint64(1)},
		{true, true},
		{time.Time{}, time.Time{}},
		{strPtr, str},
		{strPtrNil, nil},
		{timePtr, Time},
		{timePtrNil, nil},
		{int64Ptr, Int64},
		{int64PtrNil, nil},
		{float64Ptr, Float64},
		{float64PtrNil, nil},
		{intPtr, Int},
		{intPtrNil, nil},
		{boolPtr, Bool},
		{boolPtrNil, nil},
	}

	for i, tt := range testcases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			col := QueryResultCol{ColName: "name", Value: tt.value}
			if col.ExtractValue() != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, col.ExtractValue())
			}
		})
	}
}
