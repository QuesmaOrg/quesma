// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package doris

import (
	"reflect"
	"strings"
	"time"
)

// this is catch all type for all types we do not exlicitly support
type UnknownType struct{}

type DorisTypeResolver struct{}

func (r *DorisTypeResolver) ResolveType(dorisTypeName string) reflect.Type {
	dorisTypeName = strings.ToLower(dorisTypeName)
	switch dorisTypeName {
	case "char", "varchar", "string", "text":
		return reflect.TypeOf("")
	case "date", "datetime", "datev2", "datetimev2":
		return reflect.TypeOf(time.Time{})
	case "tinyint", "smallint", "int":
		return reflect.TypeOf(int32(0))
	case "bigint":
		return reflect.TypeOf(int64(0))
	case "largeint":
		return reflect.TypeOf("") // LargeInt is typically handled as string due to size
	case "boolean":
		return reflect.TypeOf(true)
	case "float":
		return reflect.TypeOf(float32(0))
	case "double":
		return reflect.TypeOf(float64(0))
	case "decimal", "decimalv2":
		return reflect.TypeOf("") // Decimals often handled as strings for precision
	case "json":
		return reflect.TypeOf(map[string]interface{}{})
	case "hll":
		return reflect.TypeOf([]byte{}) // HLL is a binary type
	case "bitmap":
		return reflect.TypeOf([]byte{}) // Bitmap is also binary
	case "array":
		return reflect.TypeOf([]interface{}{})
	case "map":
		return reflect.TypeOf(map[string]interface{}{})
	case "struct":
		return reflect.TypeOf(map[string]interface{}{})
	case "Unknown":
		return reflect.TypeOf(UnknownType{})
	}
	return nil
}
