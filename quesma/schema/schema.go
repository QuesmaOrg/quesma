// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

type (
	Schema struct {
		Fields  map[FieldName]Field
		Aliases map[FieldName]FieldName
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

func (t FieldName) AsString() string {
	return string(t)
}

func (t TableName) AsString() string {
	return string(t)
}

func (s Schema) ResolveField(fieldName FieldName) (Field, bool) {
	if alias, exists := s.Aliases[fieldName]; exists {
		field, exists := s.Fields[alias]
		return field, exists
	}
	field, exists := s.Fields[fieldName]
	return field, exists
}
