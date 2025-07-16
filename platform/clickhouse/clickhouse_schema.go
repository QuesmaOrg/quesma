// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"reflect"
	"time"
)

// this is catch all type for all types we do not exlicitly support
type UnknownType struct{}

type ClickhouseTypeResolver struct{}

func (r *ClickhouseTypeResolver) ResolveType(clickHouseTypeName string) reflect.Type {
	switch clickHouseTypeName {
	case "String", "LowCardinality(String)", "UUID", "FixedString":
		return reflect.TypeOf("")
	case "DateTime64", "DateTime", "Date", "DateTime64(3)":
		return reflect.TypeOf(time.Time{})
	case "UInt8", "UInt16", "UInt32", "UInt64":
		return reflect.TypeOf(uint64(0))
	case "Int8", "Int16", "Int32":
		return reflect.TypeOf(int32(0))
	case "Int64":
		return reflect.TypeOf(int64(0))
	case "Float32", "Float64":
		return reflect.TypeOf(float64(0))
	case "Point":
		return reflect.TypeOf(Point{})
	case "Bool":
		return reflect.TypeOf(true)
	case "JSON":
		return reflect.TypeOf(map[string]interface{}{})
	case "Map(String, Nullable(String))", "Map(String, String)", "Map(LowCardinality(String), String)", "Map(LowCardinality(String), Nullable(String))":
		return reflect.TypeOf(map[string]string{})
	case "Unknown":
		return reflect.TypeOf(UnknownType{})
	}
	return nil
}
