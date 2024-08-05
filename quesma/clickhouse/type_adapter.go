// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"quesma/schema"
	"strings"
)

type SchemaTypeAdapter struct {
}

func (c SchemaTypeAdapter) Convert(s string) (schema.Type, bool) {
	for isArray(s) {
		s = arrayType(s)
	}
	switch {
	case strings.HasPrefix(s, "Unknown"):
		return schema.TypeText, true // TODO
	case strings.HasPrefix(s, "Tuple"):
		return schema.TypeObject, true
	}

	switch s {
	case "String", "LowCardinality(String)":
		return schema.TypeKeyword, true
	case "Int", "Int8", "Int16", "Int32", "Int64":
		return schema.TypeLong, true
	case "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256":
		return schema.TypeUnsignedLong, true
	case "Bool":
		return schema.TypeBoolean, true
	case "Float32", "Float64":
		return schema.TypeFloat, true
	case "DateTime", "DateTime64":
		return schema.TypeTimestamp, true
	case "Date":
		return schema.TypeDate, true
	case "Point":
		return schema.TypePoint, true
	default:
		return schema.TypeUnknown, false
	}
}

func isArray(s string) bool {
	return strings.HasPrefix(s, "Array(") && strings.HasSuffix(s, ")")
}

func arrayType(s string) string {
	return s[6 : len(s)-1]
}
