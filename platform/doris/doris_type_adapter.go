package doris

import (
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"strings"
)

type DorisSchemaTypeAdapter struct {
	defaultStringColumnType string
}

func NewDorisSchemaTypeAdapter(defaultType string) DorisSchemaTypeAdapter {
	return DorisSchemaTypeAdapter{
		defaultStringColumnType: defaultType,
	}
}

func (c DorisSchemaTypeAdapter) Convert(s string) (schema.QuesmaType, bool) {
	s = strings.ToUpper(s)

	for isArray(s) {
		s = arrayType(s)
		s = strings.ToUpper(s)
	}

	switch {
	case strings.HasPrefix(s, "UNKNOWN"):
		return schema.QuesmaTypeUnknown, true
	case strings.HasPrefix(s, "STRUCT"):
		return schema.QuesmaTypeObject, true
	case strings.HasPrefix(s, "MAP"):
		return schema.QuesmaTypeMap, true
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
	case "BOOLEAN":
		return schema.QuesmaTypeBoolean, true
	case "TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT":
		return schema.QuesmaTypeLong, true
	case "FLOAT", "DOUBLE":
		return schema.QuesmaTypeFloat, true
	case "DECIMAL", "DECIMAL32", "DECIMAL64", "DECIMAL128":
		return schema.QuesmaTypeKeyword, true // mapping to keyword type
	case "STRING", "CHAR", "VARCHAR":
		return schema.QuesmaTypeText, true
	case "DATE", "DATEV2", "DATETIME", "DATETIMEV2":
		return schema.QuesmaTypeDate, true
	case "JSON", "VARIANT":
		return schema.QuesmaTypeObject, true
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
