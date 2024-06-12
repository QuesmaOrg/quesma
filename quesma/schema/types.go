package schema

import (
	"mitmproxy/quesma/elasticsearch/elasticsearch_field_types"
	"strings"
)

const (
	// TODO add more and review existing
	TypeText         Type = "text"
	TypeKeyword      Type = "keyword"
	TypeLong         Type = "long"
	TypeUnsignedLong Type = "unsigned_long"
	TypeTimestamp    Type = "timestamp"
	TypeDate         Type = "date"
	TypeFloat        Type = "float"
	TypeBoolean      Type = "bool"
	TypeJSON         Type = "json"
	TypeArray        Type = "array"
	TypeMap          Type = "map"
	TypeIp           Type = "ip"
	TypePoint        Type = "point"
)

func IsValid(t string) (Type, bool) {
	switch t {
	case "text":
		return TypeText, true
	case "keyword":
		return TypeKeyword, true
	case "long":
		return TypeLong, true
	case "timestamp":
		return TypeTimestamp, true
	case "date":
		return TypeDate, true
	case "float":
		return TypeFloat, true
	case "bool":
		return TypeBoolean, true
	case "json":
		return TypeJSON, true
	case "array":
		return TypeArray, true
	case "map":
		return TypeMap, true
	case "ip":
		return TypeIp, true
	case "point":
		return TypePoint, true
	default:
		return "", false
	}
}

type ClickhouseTypeAdapter struct {
}

func (c ClickhouseTypeAdapter) Convert(s string) (Type, bool) {
	if strings.HasPrefix(s, "Unknown") {
		return TypeText, true // TODO
	}
	switch s {
	case "String", "LowCardinality(String)":
		return TypeText, true
	case "Int", "Int8", "Int16", "Int32", "Int64":
		return TypeLong, true
	case "Uint8", "Uint16", "Uint32", "Uint64", "Uint128", "Uint256":
		return TypeUnsignedLong, true
	case "Bool":
		return TypeBoolean, true
	case "Float32", "Float64":
		return TypeFloat, true
	case "DateTime", "DateTime64":
		return TypeTimestamp, true
	case "Date":
		return TypeDate, true
	default:
		return "", false
	}
}

type ElasticsearchTypeAdapter struct {
}

func (e ElasticsearchTypeAdapter) Convert(s string) (Type, bool) {
	switch s {
	case elasticsearch_field_types.FieldTypeText:
		return TypeText, true
	case elasticsearch_field_types.FieldTypeKeyword:
		return TypeKeyword, true
	case elasticsearch_field_types.FieldTypeLong:
		return TypeLong, true
	case elasticsearch_field_types.FieldTypeDate:
		return TypeDate, true
	case elasticsearch_field_types.FieldTypeDateNanos:
		return TypeDate, true
	case elasticsearch_field_types.FieldTypeDouble:
		return TypeFloat, true
	case elasticsearch_field_types.FieldTypeBoolean:
		return TypeBoolean, true
	case elasticsearch_field_types.FieldTypeIp:
		return TypeIp, true
	default:
		return "", false
	}
}
