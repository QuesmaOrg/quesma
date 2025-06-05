// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"github.com/QuesmaOrg/quesma/platform/schema"
	"strings"
)

type SchemaTypeAdapter struct {
}

func (c SchemaTypeAdapter) Convert(s string) (schema.QuesmaType, bool) {
	for isArray(s) {
		s = arrayType(s)
	}
	switch {
	case strings.HasPrefix(s, "Unknown"):
		return schema.QuesmaTypeUnknown, true
	case strings.HasPrefix(s, "Tuple"):
		return schema.QuesmaTypeObject, true
	}

	switch s {
	case "String": // This should be treated as a text type (full text search). But ingest do not distinguish between LowCardinality and String.
		return schema.QuesmaTypeKeyword, true
	case "LowCardinality(String)", "UUID", "FixedString":
		return schema.QuesmaTypeKeyword, true
	case "Int", "Int8", "Int16", "Int32", "Int64":
		return schema.QuesmaTypeLong, true
	case "UInt8", "UInt16", "UInt32", "UInt64", "UInt128", "UInt256":
		return schema.QuesmaTypeInteger, true
	case "Bool":
		return schema.QuesmaTypeBoolean, true
	case "Float32", "Float64":
		return schema.QuesmaTypeFloat, true
	case "DateTime", "DateTime64":
		return schema.QuesmaTypeTimestamp, true
	case "Date":
		return schema.QuesmaTypeDate, true
	case "Point":
		return schema.QuesmaTypePoint, true
	case "Map(String, Nullable(String))", "Map(String, String)", "Map(LowCardinality(String), Nullable(String))", "Map(LowCardinality(String), String)",
		"Map(String, Int)", "Map(LowCardinality(String), Int)", "Map(String, Nullable(Int))", "Map(LowCardinality(String), Nullable(Int))":
		return schema.QuesmaTypeMap, true
	default:
		return schema.QuesmaTypeUnknown, false
	}
}

func isArray(s string) bool {
	return strings.HasPrefix(s, "Array(") && strings.HasSuffix(s, ")")
}

func arrayType(s string) string {
	return s[6 : len(s)-1]
}
