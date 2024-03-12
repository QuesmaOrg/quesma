package clickhouse

import (
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
			want: &Column{Name: "is_critical", Type: BaseType{Name: "Bool", goType: reflect.TypeOf(true)}},
		},
		{
			name: "UInt64",
			args: args{colName: "count", colType: "UInt64"},
			want: &Column{Name: "count", Type: BaseType{Name: "UInt64", goType: reflect.TypeOf(uint64(0))}},
		},
		{
			name: "Int64",
			args: args{colName: "count", colType: "Int64"},
			want: &Column{Name: "count", Type: BaseType{Name: "Int64", goType: reflect.TypeOf(int64(0))}},
		},
		{
			name: "String",
			args: args{colName: "severity", colType: "String"},
			want: &Column{Name: "severity", Type: BaseType{Name: "String", goType: reflect.TypeOf("")}},
		},
		{
			name: "Nullable(String)",
			args: args{colName: "severity", colType: "String"},
			want: &Column{Name: "severity", Type: BaseType{Name: "String", goType: reflect.TypeOf("")}},
		},
		{
			name: "LowCardinality(String)",
			args: args{colName: "severity", colType: "String"},
			want: &Column{Name: "severity", Type: BaseType{Name: "String", goType: reflect.TypeOf("")}},
		},
		{
			name: "DateTime",
			args: args{colName: "@timestamp", colType: "DateTime"},
			want: &Column{Name: "@timestamp", Type: BaseType{Name: "DateTime", goType: reflect.TypeOf(time.Time{})}},
		},
		{
			name: "DateTime64",
			args: args{colName: "@timestamp", colType: "DateTime64"},
			want: &Column{Name: "@timestamp", Type: BaseType{Name: "DateTime64", goType: reflect.TypeOf(time.Time{})}},
		},
		{
			name: "DateTime64(3)",
			args: args{colName: "@timestamp", colType: "DateTime64"},
			want: &Column{Name: "@timestamp", Type: BaseType{Name: "DateTime64", goType: reflect.TypeOf(time.Time{})}},
		},
		{
			name: "Array(String)",
			args: args{colName: "tags", colType: "Array(String)"},
			want: &Column{Name: "tags", Type: CompoundType{Name: "Array", BaseType: BaseType{Name: "String", goType: reflect.TypeOf("")}}},
		},
		{
			name: "Array(DateTime64)",
			args: args{colName: "tags", colType: "Array(DateTime64)"},
			want: &Column{Name: "tags", Type: CompoundType{Name: "Array", BaseType: BaseType{Name: "DateTime64", goType: reflect.TypeOf(time.Time{})}}},
		},
		{
			name: "Tuple",
			args: args{colName: "tuple", colType: "Tuple(taxful_price Nullable(Float64), product_id Nullable(Int64), category Nullable(String), created_on DateTime64(3), manufacturer Nullable(String))"},
			want: &Column{
				Name: "tuple",
				Type: MultiValueType{
					Name: "Tuple",
					Cols: []*Column{
						{Name: "taxful_price", Type: BaseType{Name: "Float64", goType: reflect.TypeOf(float64(0))}},
						{Name: "product_id", Type: BaseType{Name: "Int64", goType: reflect.TypeOf(int64(0))}},
						{Name: "category", Type: BaseType{Name: "String", goType: reflect.TypeOf("")}},
						{Name: "created_on", Type: BaseType{Name: "DateTime64", goType: reflect.TypeOf(time.Time{})}},
						{Name: "manufacturer", Type: BaseType{Name: "String", goType: reflect.TypeOf("")}},
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
							{Name: "taxful_price", Type: BaseType{Name: "Float64", goType: reflect.TypeOf(float64(0))}},
							{Name: "product_id", Type: BaseType{Name: "Int64", goType: reflect.TypeOf(int64(0))}},
							{Name: "category", Type: BaseType{Name: "String", goType: reflect.TypeOf("")}},
							{Name: "created_on", Type: BaseType{Name: "DateTime64", goType: reflect.TypeOf(time.Time{})}},
							{Name: "manufacturer", Type: BaseType{Name: "String", goType: reflect.TypeOf("")}},
						},
					},
				},
			},
		},
		{
			name: "Array(Tuple(...)) used to panic",
			args: args{colName: "array", colType: "Array(Tuple(taxful_price Nullable(Float64), product_id Nullable(Int64), category Nullable(String), created_on DateTime64(3)))"},
			want: &Column{
				Name: "array",
				Type: CompoundType{
					Name: "Array",
					BaseType: MultiValueType{
						Name: "Tuple",
						Cols: []*Column{
							{Name: "taxful_price", Type: BaseType{Name: "Float64", goType: reflect.TypeOf(float64(0))}},
							{Name: "product_id", Type: BaseType{Name: "Int64", goType: reflect.TypeOf(int64(0))}},
							{Name: "category", Type: BaseType{Name: "String", goType: reflect.TypeOf("")}},
							{Name: "created_on", Type: BaseType{Name: "DateTime64", goType: reflect.TypeOf(time.Time{})}},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, resolveColumn(tt.args.colName, tt.args.colType), "resolveColumn(%v, %v)", tt.args.colName, tt.args.colType)
		})
	}
}
