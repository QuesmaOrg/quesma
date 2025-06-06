// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"strings"
)

type SchemaTypeAdapter struct {
	defaultStringColumnType string
}

func NewSchemaTypeAdapter(defaultType string) SchemaTypeAdapter {

	return SchemaTypeAdapter{
		defaultStringColumnType: defaultType,
	}
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
	case "String":
		switch c.defaultStringColumnType {

		// empty if for testing purposes, in production it should always be set
		case "", "text":
			return schema.QuesmaTypeText, true
		case "keyword":
			return schema.QuesmaTypeKeyword, true
		default:
			logger.Error().Msgf("Unknown field type %s", c.defaultStringColumnType)
			return schema.QuesmaTypeUnknown, false
		}

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
