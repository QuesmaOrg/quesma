// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

import (
	"strings"
)

// FieldSource is an enum that represents the source of a field in the schema
type FieldSource int

const (
	FieldSourceIngest FieldSource = iota
	FieldSourceMapping
)

type (
	Schema struct {
		Fields             map[FieldName]Field
		Aliases            map[FieldName]FieldName
		primaryKey         *FieldName // nil if no primary key
		ExistsInDataSource bool

		// DatabaseName is the name of the database/schema in the data source,
		// which in query prepends the physical table name e.g. 'FROM databaseName.tableName'
		DatabaseName string
	}
	Field struct {
		// PropertyName is how users refer to the field
		PropertyName FieldName
		// InternalPropertyName is how the field is represented in the data source
		InternalPropertyName FieldName
		InternalPropertyType string
		Type                 QuesmaType
		Origin               FieldSource
	}
	IndexName string
	FieldName string
)

func NewSchemaWithAliases(fields map[FieldName]Field, aliases map[FieldName]FieldName, existsInDataSource bool, databaseName string) Schema {

	return Schema{
		Fields:             fields,
		Aliases:            aliases,
		ExistsInDataSource: existsInDataSource,
		DatabaseName:       databaseName,
	}
}

func NewSchema(fields map[FieldName]Field, existsInDataSource bool, databaseName string) Schema {
	return NewSchemaWithAliases(fields, map[FieldName]FieldName{}, existsInDataSource, databaseName)
}

func (f FieldName) AsString() string {
	return string(f)
}

func (f FieldName) Components() []string {
	return strings.Split(f.AsString(), ".")
}

func (t IndexName) AsString() string {
	return string(t)
}

func (s Schema) ResolveFieldByInternalName(fieldName string) (Field, bool) {

	for _, field := range s.Fields {
		if field.InternalPropertyName.AsString() == fieldName {
			return field, true
		}
	}
	return Field{}, false
}

func (s Schema) ResolveField(fieldName string) (Field, bool) {
	if alias, exists := s.Aliases[FieldName(fieldName)]; exists {
		field, exists := s.Fields[alias]
		return field, exists
	}
	field, exists := s.Fields[FieldName(fieldName)]
	return field, exists
}

func (s Schema) GetPrimaryKey() *FieldName {
	return s.primaryKey
}
