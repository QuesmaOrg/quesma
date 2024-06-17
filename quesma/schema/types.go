package schema

import "slices"

var (
	// TODO add more and review existing
	TypeText         = Type{Name: "text", Properties: []TypeProperty{Searchable, FullText}}
	TypeKeyword      = Type{Name: "keyword", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeLong         = Type{Name: "long", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeUnsignedLong = Type{Name: "unsigned_long", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeTimestamp    = Type{Name: "timestamp", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeDate         = Type{Name: "date", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeFloat        = Type{Name: "float", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeBoolean      = Type{Name: "boolean", Properties: []TypeProperty{Searchable, Aggregatable}}
	TypeObject       = Type{Name: "object", Properties: []TypeProperty{Searchable}}
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

func (t Type) IsAggregatable() bool {
	return slices.Contains(t.Properties, Aggregatable)
}

func (t Type) IsSearchable() bool {
	return slices.Contains(t.Properties, Searchable)
}

func (t Type) IsFullText() bool {
	return slices.Contains(t.Properties, FullText)
}

type (
	Type struct {
		Name       string
		Properties []TypeProperty
	}
	TypeProperty string
)

func (t Type) String() string {
	return t.Name
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
		return TypeObject, true
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
