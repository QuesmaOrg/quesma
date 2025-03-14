// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

import (
	"slices"
)

type (
	QuesmaType struct {
		Name       string
		Properties []QuesmaTypeProperty
	}
	QuesmaTypeProperty string
)

var (
	QuesmaTypeText         = QuesmaType{Name: "text", Properties: []QuesmaTypeProperty{Searchable, FullText}}
	QuesmaTypeKeyword      = QuesmaType{Name: "keyword", Properties: []QuesmaTypeProperty{Searchable, Aggregatable}}
	QuesmaTypeInteger      = QuesmaType{Name: "integer", Properties: []QuesmaTypeProperty{Searchable, Aggregatable}}
	QuesmaTypeLong         = QuesmaType{Name: "long", Properties: []QuesmaTypeProperty{Searchable, Aggregatable}}
	QuesmaTypeUnsignedLong = QuesmaType{Name: "unsigned_long", Properties: []QuesmaTypeProperty{Searchable, Aggregatable}}
	QuesmaTypeTimestamp    = QuesmaType{Name: "timestamp", Properties: []QuesmaTypeProperty{Searchable, Aggregatable}}
	QuesmaTypeDate         = QuesmaType{Name: "date", Properties: []QuesmaTypeProperty{Searchable, Aggregatable}}
	QuesmaTypeFloat        = QuesmaType{Name: "float", Properties: []QuesmaTypeProperty{Searchable, Aggregatable}}
	QuesmaTypeBoolean      = QuesmaType{Name: "boolean", Properties: []QuesmaTypeProperty{Searchable, Aggregatable}}
	QuesmaTypeObject       = QuesmaType{Name: "object", Properties: []QuesmaTypeProperty{}}
	QuesmaTypeArray        = QuesmaType{Name: "array", Properties: []QuesmaTypeProperty{Searchable}}
	QuesmaTypeMap          = QuesmaType{Name: "map", Properties: []QuesmaTypeProperty{}}
	QuesmaTypeIp           = QuesmaType{Name: "ip", Properties: []QuesmaTypeProperty{Searchable, Aggregatable}}
	QuesmaTypePoint        = QuesmaType{Name: "point", Properties: []QuesmaTypeProperty{Searchable, Aggregatable}}
	QuesmaTypeUnknown      = QuesmaType{Name: "unknown", Properties: []QuesmaTypeProperty{Searchable}}
)

const (
	Aggregatable QuesmaTypeProperty = "aggregatable"
	Searchable   QuesmaTypeProperty = "searchable"
	FullText     QuesmaTypeProperty = "full_text"
)

func (t QuesmaType) Equal(t2 QuesmaType) bool {
	return t.Name == t2.Name
}

func (t QuesmaType) IsAggregatable() bool {
	return slices.Contains(t.Properties, Aggregatable)
}

func (t QuesmaType) IsSearchable() bool {
	return slices.Contains(t.Properties, Searchable)
}

func (t QuesmaType) IsFullText() bool {
	return slices.Contains(t.Properties, FullText)
}

func (t QuesmaType) String() string {
	return t.Name
}

func ParseQuesmaType(t string) (QuesmaType, bool) {
	switch t {
	case QuesmaTypeText.Name:
		return QuesmaTypeText, true
	case QuesmaTypeKeyword.Name:
		return QuesmaTypeKeyword, true
	case QuesmaTypeLong.Name:
		return QuesmaTypeLong, true
	case QuesmaTypeTimestamp.Name:
		return QuesmaTypeTimestamp, true
	case QuesmaTypeDate.Name:
		return QuesmaTypeDate, true
	case QuesmaTypeFloat.Name:
		return QuesmaTypeFloat, true
	case QuesmaTypeBoolean.Name, "bool":
		return QuesmaTypeBoolean, true
	case QuesmaTypeObject.Name, "json":
		return QuesmaTypeObject, true
	case QuesmaTypeArray.Name:
		return QuesmaTypeArray, true
	case QuesmaTypeMap.Name:
		return QuesmaTypeMap, true
	case QuesmaTypeIp.Name:
		return QuesmaTypeIp, true
	case QuesmaTypePoint.Name, "geo_point":
		return QuesmaTypePoint, true
	default:
		return QuesmaTypeUnknown, false
	}
}
