package schema

import (
	"mitmproxy/quesma/elasticsearch/elasticsearch_field_types"
	"slices"
	"strings"
)

var (
	// TODO add more and review existing
	TypeText         = Type{Name: "text", Properties: []TypeProperty{Searchable, FullText}}
	TypeKeyword      = Type{Name: "keyword", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeLong         = Type{Name: "long", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeUnsignedLong = Type{Name: "unsigned_long", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeTimestamp    = Type{Name: "timestamp", Properties: []TypeProperty{Searchable}}
	TypeDate         = Type{Name: "date", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeFloat        = Type{Name: "float", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeBoolean      = Type{Name: "boolean", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeJSON         = Type{Name: "json", Properties: []TypeProperty{Searchable}}
	TypeArray        = Type{Name: "array", Properties: []TypeProperty{Searchable}}
	TypeMap          = Type{Name: "map", Properties: []TypeProperty{Searchable}}
	TypeIp           = Type{Name: "ip", Properties: []TypeProperty{Searchable}}
	TypePoint        = Type{Name: "point", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeUnknown      = Type{Name: "unknown", Properties: []TypeProperty{Searchable}}
)

const (
	Aggregatable TypeProperty = "aggregatable"
	Searchable   TypeProperty = "searchable"
	FullText     TypeProperty = "full_text"
)

type (
	Type struct {
		Name       string
		Properties []TypeProperty
	}
	TypeProperty string
)

func (t Type) IsAggregatable() bool {
	return slices.Contains(t.Properties, Aggregatable)
}

func (t Type) IsSearchable() bool {
	return slices.Contains(t.Properties, Searchable)
}

func (t Type) IsFullText() bool {
	return slices.Contains(t.Properties, FullText)
}

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
		return TypeUnknown, false
	}
}

type ClickhouseTypeAdapter struct {
}

func (c ClickhouseTypeAdapter) ConvertToQuesma(s string) (Type, bool) {
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
		return TypeUnknown, false
	}
}

type ElasticsearchTypeAdapter struct {
}

func (e ElasticsearchTypeAdapter) ConvertToQuesma(s string) (Type, bool) {
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
		return TypeUnknown, false
	}
}

func (e ElasticsearchTypeAdapter) ConvertFrom(t Type) string {
	switch t.Name {
	case TypeText.Name:
		return elasticsearch_field_types.FieldTypeText
	case TypeKeyword.Name:
		return elasticsearch_field_types.FieldTypeKeyword
	case TypeLong.Name:
		return elasticsearch_field_types.FieldTypeLong
	case TypeDate.Name:
		return elasticsearch_field_types.FieldTypeDate
	case TypeFloat.Name:
		return elasticsearch_field_types.FieldTypeDouble
	case TypeBoolean.Name:
		return elasticsearch_field_types.FieldTypeBoolean
	case TypeIp.Name:
		return elasticsearch_field_types.FieldTypeIp
	default:
		return elasticsearch_field_types.FieldTypeText
	}
}
