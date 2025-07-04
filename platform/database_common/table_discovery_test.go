// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package database_common

import (
	"github.com/QuesmaOrg/quesma/platform/util"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func Test_resolveColumn(t *testing.T) {
	type args struct {
		colName string
		colType string
	}
	tests := []struct {
		name string
		args args
		want *Column
	}{
		{
			name: "Bool",
			args: args{colName: "is_critical", colType: "Bool"},
			want: &Column{Name: "is_critical", Type: BaseType{Name: "Bool", GoType: reflect.TypeOf(true)}},
		},
		{
			name: "UInt64",
			args: args{colName: "count", colType: "UInt64"},
			want: &Column{Name: "count", Type: BaseType{Name: "UInt64", GoType: reflect.TypeOf(uint64(0))}},
		},
		{
			name: "Int64",
			args: args{colName: "count", colType: "Int64"},
			want: &Column{Name: "count", Type: BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0))}},
		},
		{
			name: "String",
			args: args{colName: "severity", colType: "String"},
			want: &Column{Name: "severity", Type: BaseType{Name: "String", GoType: reflect.TypeOf("")}},
		},
		{
			name: "Nullable(String)",
			args: args{colName: "severity", colType: "String"},
			want: &Column{Name: "severity", Type: BaseType{Name: "String", GoType: reflect.TypeOf("")}},
		},
		{
			name: "LowCardinality(String)",
			args: args{colName: "severity", colType: "String"},
			want: &Column{Name: "severity", Type: BaseType{Name: "String", GoType: reflect.TypeOf("")}},
		},
		{
			name: "DateTime",
			args: args{colName: "@timestamp", colType: "DateTime"},
			want: &Column{Name: "@timestamp", Type: BaseType{Name: "DateTime", GoType: reflect.TypeOf(time.Time{})}},
		},
		{
			name: "DateTime64",
			args: args{colName: "@timestamp", colType: "DateTime64"},
			want: &Column{Name: "@timestamp", Type: BaseType{Name: "DateTime64", GoType: reflect.TypeOf(time.Time{})}},
		},
		{
			name: "Date",
			args: args{colName: "@timestamp", colType: "Date"},
			want: &Column{Name: "@timestamp", Type: BaseType{Name: "Date", GoType: reflect.TypeOf(time.Time{})}},
		},
		{
			name: "DateTime64(3)",
			args: args{colName: "@timestamp", colType: "DateTime64"},
			want: &Column{Name: "@timestamp", Type: BaseType{Name: "DateTime64", GoType: reflect.TypeOf(time.Time{})}},
		},
		{
			name: "Array(String)",
			args: args{colName: "tags", colType: "Array(String)"},
			want: &Column{Name: "tags", Type: CompoundType{Name: "Array", BaseType: BaseType{Name: "String", GoType: reflect.TypeOf("")}}},
		},
		{
			name: "Array(DateTime64)",
			args: args{colName: "tags", colType: "Array(DateTime64)"},
			want: &Column{Name: "tags", Type: CompoundType{Name: "Array", BaseType: BaseType{Name: "DateTime64", GoType: reflect.TypeOf(time.Time{})}}},
		},

		{
			name: "Tuple",
			args: args{colName: "tuple", colType: "Tuple(taxful_price Float64, product_id Int64, category String, created_on DateTime64(3), manufacturer String)"},
			want: &Column{
				Name: "tuple",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						{Name: "taxful_price", Type: BaseType{Name: "Float64", GoType: reflect.TypeOf(float64(0))}},
						{Name: "product_id", Type: BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0))}},
						{Name: "category", Type: BaseType{Name: "String", GoType: reflect.TypeOf("")}},
						{Name: "created_on", Type: BaseType{Name: "DateTime64", GoType: reflect.TypeOf(time.Time{})}},
						{Name: "manufacturer", Type: BaseType{Name: "String", GoType: reflect.TypeOf("")}},
					},
				},
			},
		},
		{
			name: "Array(Tuple(...))",
			args: args{colName: "array", colType: "Array(Tuple(taxful_price Float64, product_id Int64, category String, created_on DateTime64(3), manufacturer String))"},
			want: &Column{
				Name: "array",
				Type: CompoundType{
					Name: "Array",
					BaseType: MultiValueType{
						Name: "Tuple",
						Cols: []*Column{
							{Name: "taxful_price", Type: BaseType{Name: "Float64", GoType: reflect.TypeOf(float64(0))}},
							{Name: "product_id", Type: BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0))}},
							{Name: "category", Type: BaseType{Name: "String", GoType: reflect.TypeOf("")}},
							{Name: "created_on", Type: BaseType{Name: "DateTime64", GoType: reflect.TypeOf(time.Time{})}},
							{Name: "manufacturer", Type: BaseType{Name: "String", GoType: reflect.TypeOf("")}},
						},
					},
				},
			},
		},
		{
			name: "Array(Tuple(...)) used to panic",
			args: args{colName: "array", colType: "Array(Tuple(taxful_price Float64, product_id Int64, category String, created_on DateTime64(3)))"},
			want: &Column{
				Name: "array",
				Type: CompoundType{
					Name: "Array",
					BaseType: MultiValueType{
						Name: "Tuple",
						Cols: []*Column{
							{Name: "taxful_price", Type: BaseType{Name: "Float64", GoType: reflect.TypeOf(float64(0))}},
							{Name: "product_id", Type: BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0))}},
							{Name: "category", Type: BaseType{Name: "String", GoType: reflect.TypeOf("")}},
							{Name: "created_on", Type: BaseType{Name: "DateTime64", GoType: reflect.TypeOf(time.Time{})}},
						},
					},
				},
			},
		},
		{
			name: "Array(DateTime64(3))",
			args: args{colName: "tags", colType: "Array(DateTime64(3))"},
			want: &Column{Name: "tags", Type: CompoundType{Name: "Array", BaseType: BaseType{Name: "DateTime64(3)", GoType: reflect.TypeOf(time.Time{})}}},
		},
	}

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {
			assert.Equalf(t, tt.want, ResolveColumn(tt.args.colName, tt.args.colType), "ResolveColumn(%v, %v)", tt.args.colName, tt.args.colType)
		})
	}
}

func Test_resolveColumn_Nullable(t *testing.T) {
	type args struct {
		colName string
		colType string
	}
	tests := []struct {
		name string
		args args
		want *Column
	}{
		{
			name: "BaseType 1",
			args: args{colName: "is_critical", colType: "Nullable(Bool)"},
			want: &Column{Name: "is_critical", Type: BaseType{Name: "Bool", GoType: reflect.TypeOf(true), Nullable: true}},
		},
		{
			name: "BaseType 2",
			args: args{colName: "count", colType: "Nullable(UInt64)"},
			want: &Column{Name: "count", Type: BaseType{Name: "UInt64", GoType: reflect.TypeOf(uint64(0)), Nullable: true}},
		},
		{
			name: "LowCardinality(String)",
			args: args{colName: "severity", colType: "Nullable(String)"},
			want: &Column{Name: "severity", Type: BaseType{Name: "String", GoType: reflect.TypeOf(""), Nullable: true}},
		},
		{
			name: "DateTime64(3)",
			args: args{colName: "@timestamp", colType: "Nullable(DateTime64)"},
			want: &Column{Name: "@timestamp", Type: BaseType{Name: "DateTime64", GoType: reflect.TypeOf(time.Time{}), Nullable: true}},
		},
		{
			name: "Array(Nullable(String))",
			args: args{colName: "tags", colType: "Array(Nullable(String))"},
			want: &Column{Name: "tags", Type: CompoundType{Name: "Array", BaseType: BaseType{Name: "String", GoType: reflect.TypeOf(""), Nullable: true}}},
		},

		{
			name: "Array(DateTime64)",
			args: args{colName: "tags", colType: "Array(Nullable(DateTime64))"},
			want: &Column{Name: "tags", Type: CompoundType{Name: "Array", BaseType: BaseType{Name: "DateTime64", GoType: reflect.TypeOf(time.Time{}), Nullable: true}}},
		},
		{
			name: "Tuple",
			args: args{colName: "tuple", colType: "Tuple(taxful_price Nullable(Float64), product_id Nullable(Int64), category Nullable(String), created_on DateTime64(3), manufacturer Nullable(String))"},
			want: &Column{
				Name: "tuple",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						{Name: "taxful_price", Type: BaseType{Name: "Float64", GoType: reflect.TypeOf(float64(0)), Nullable: true}},
						{Name: "product_id", Type: BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0)), Nullable: true}},
						{Name: "category", Type: BaseType{Name: "String", GoType: reflect.TypeOf(""), Nullable: true}},
						{Name: "created_on", Type: BaseType{Name: "DateTime64", GoType: reflect.TypeOf(time.Time{})}},
						{Name: "manufacturer", Type: BaseType{Name: "String", GoType: reflect.TypeOf(""), Nullable: true}},
					},
				},
			},
		},
		{
			name: "Array(Tuple(...))",
			args: args{colName: "array", colType: "Array(Tuple(taxful_price Nullable(Float64), product_id Nullable(Int64), category Nullable(String), created_on DateTime64(3), manufacturer Nullable(String)))"},
			want: &Column{
				Name: "array",
				Type: CompoundType{
					Name: "Array",
					BaseType: MultiValueType{
						Name: "Tuple",
						Cols: []*Column{
							{Name: "taxful_price", Type: BaseType{Name: "Float64", GoType: reflect.TypeOf(float64(0)), Nullable: true}},
							{Name: "product_id", Type: BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0)), Nullable: true}},
							{Name: "category", Type: BaseType{Name: "String", GoType: reflect.TypeOf(""), Nullable: true}},
							{Name: "created_on", Type: BaseType{Name: "DateTime64", GoType: reflect.TypeOf(time.Time{})}},
							{Name: "manufacturer", Type: BaseType{Name: "String", GoType: reflect.TypeOf(""), Nullable: true}},
						},
					},
				},
			},
		},
		{
			name: "Array(Array(Int64))",
			args: args{colName: "array_of_arrays", colType: "Array(Array(Int64))"},
			want: &Column{
				Name: "array_of_arrays",
				Type: CompoundType{
					Name: "Array",
					BaseType: CompoundType{
						Name:     "Array",
						BaseType: BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0))},
					},
				},
			},
		},
		{
			name: "Array(Array(Array(Array(String))))",
			args: args{colName: "deeply_nested_array", colType: "Array(Array(Array(Array(String))))"},
			want: &Column{
				Name: "deeply_nested_array",
				Type: CompoundType{
					Name: "Array",
					BaseType: CompoundType{
						Name: "Array",
						BaseType: CompoundType{
							Name: "Array",
							BaseType: CompoundType{
								Name:     "Array",
								BaseType: BaseType{Name: "String", GoType: reflect.TypeOf("")},
							},
						},
					},
				},
			},
		},
		{
			name: "Array(Array(Tuple(...)))",
			args: args{colName: "nested_array_tuple", colType: "Array(Array(Tuple(group_a Tuple(field_a Nullable(Int64), field_b Nullable(Int64), field_c Nullable(Int64), field_d Nullable(Int64)), group_b Tuple(field_x Nullable(String)))))"},
			want: &Column{
				Name: "nested_array_tuple",
				Type: CompoundType{
					Name: "Array",
					BaseType: CompoundType{
						Name: "Array",
						BaseType: MultiValueType{
							Name: "Tuple",
							Cols: []*Column{
								{Name: "group_a", Type: MultiValueType{
									Name: "Tuple",
									Cols: []*Column{
										{Name: "field_a", Type: BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0)), Nullable: true}},
										{Name: "field_b", Type: BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0)), Nullable: true}},
										{Name: "field_c", Type: BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0)), Nullable: true}},
										{Name: "field_d", Type: BaseType{Name: "Int64", GoType: reflect.TypeOf(int64(0)), Nullable: true}},
									},
								}},
								{Name: "group_b", Type: MultiValueType{
									Name: "Tuple",
									Cols: []*Column{
										{Name: "field_x", Type: BaseType{Name: "String", GoType: reflect.TypeOf(""), Nullable: true}},
									},
								}},
							},
						},
					},
				},
			},
		},
	}

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {
			assert.Equalf(t, tt.want, ResolveColumn(tt.args.colName, tt.args.colType), "ResolveColumn(%v, %v)", tt.args.colName, tt.args.colType)
		})
	}
}

func TestExtractMapValueType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		hasError bool
	}{
		{"Map(String, Int32)", "Int32", false},
		{"Map(String, Nullable(String))", "Nullable(String)", false},
		{"Map(String, Array(UInt8))", "Array(UInt8)", false},
		{"Map(LowCardinality(String), UInt64)", "UInt64", false},
		{"Map(LowCardinality(String), Decimal(10,2))", "Decimal(10,2)", false},
		{"Map(LowCardinality(String), FixedString(16))", "FixedString(16)", false},
		{"Map(String,)", "", true},                                       // Invalid format
		{"Map(LowCardinality(String),)", "", true},                       // Invalid format
		{"Map(String)", "", true},                                        // Missing value type
		{"RandomType(String, Int32)", "", true},                          // Not a Map type
		{"Map(String, Map(String, Int32))", "Map(String, Int32)", false}, // Nested map
	}

	for i, test := range tests {
		t.Run(util.PrettyTestName(test.input, i), func(t *testing.T) {
			result, err := extractMapValueType(test.input)
			if (err != nil) != test.hasError {
				t.Errorf("unexpected error state for input %q: got error %v", test.input, err)
			}
			if result != test.expected {
				t.Errorf("expected %q, got %q for input %q", test.expected, result, test.input)
			}
		})
	}
}
