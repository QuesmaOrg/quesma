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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, resolveColumn(tt.args.colName, tt.args.colType), "resolveColumn(%v, %v)", tt.args.colName, tt.args.colType)
		})
	}
}
