// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

type (
	Schema struct {
		Fields              map[FieldName]Field
		Aliases             map[FieldName]FieldName
		internalNameToField map[FieldName]Field
	}
	Field struct {
		// PropertyName is how users refer to the field
		PropertyName FieldName
		// InternalPropertyName is how the field is represented in the data source
		InternalPropertyName FieldName
		Type                 Type
	}
	TableName string
	FieldName string
)

func NewSchemaWithAliases(fields map[FieldName]Field, aliases map[FieldName]FieldName) Schema {
	internalNameToField := make(map[FieldName]Field)
	for _, field := range fields {
		internalNameToField[field.InternalPropertyName] = field
	}
	return Schema{
		Fields:              fields,
		Aliases:             aliases,
		internalNameToField: internalNameToField,
	}
}

func NewSchema(fields map[FieldName]Field) Schema {
	return NewSchemaWithAliases(fields, map[FieldName]FieldName{})
}

func (t FieldName) AsString() string {
	return string(t)
}

func (t TableName) AsString() string {
	return string(t)
}

func (s Schema) ResolveFieldByInternalName(fieldName string) (Field, bool) {
	if field, exists := s.internalNameToField[FieldName(fieldName)]; exists {
		return field, true
	} else {
		return Field{}, false
	}
}

func (s Schema) ResolveField(fieldName string) (Field, bool) {
	if alias, exists := s.Aliases[FieldName(fieldName)]; exists {
		field, exists := s.Fields[alias]
		return field, exists
	}
	field, exists := s.Fields[FieldName(fieldName)]
	return field, exists
}
