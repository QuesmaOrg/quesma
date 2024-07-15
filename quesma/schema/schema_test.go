// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package schema

import (
	"reflect"
	"testing"
)

func TestSchema_ResolveField(t *testing.T) {
	tests := []struct {
		name          string
		fieldName     string
		schema        Schema
		resolvedField Field
		exists        bool
	}{
		{
			name:      "empty schema",
			fieldName: "field",
			schema:    Schema{},
			exists:    false,
		},
		{
			name:          "should resolve field",
			fieldName:     "message",
			schema:        NewSchema(map[FieldName]Field{"message": {PropertyName: "message", InternalPropertyName: "message", Type: TypeText}}),
			resolvedField: Field{PropertyName: "message", InternalPropertyName: "message", Type: TypeText},
			exists:        true,
		},
		{
			name:          "should not resolve field",
			fieldName:     "foo",
			schema:        NewSchema(map[FieldName]Field{"message": {PropertyName: "message", InternalPropertyName: "message", Type: TypeText}}),
			resolvedField: Field{},
			exists:        false,
		},
		{
			name:          "should resolve aliased field",
			fieldName:     "message_alias",
			schema:        NewSchemaWithAliases(map[FieldName]Field{"message": {PropertyName: "message", InternalPropertyName: "message", Type: TypeText}}, map[FieldName]FieldName{"message_alias": "message"}),
			resolvedField: Field{PropertyName: "message", InternalPropertyName: "message", Type: TypeText},
			exists:        true,
		},
		{
			name:          "should not resolve aliased field",
			fieldName:     "message_alias",
			schema:        NewSchemaWithAliases(map[FieldName]Field{"message": {PropertyName: "message", InternalPropertyName: "message", Type: TypeText}}, map[FieldName]FieldName{"message_alias": "foo"}),
			resolvedField: Field{},
			exists:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, exists := tt.schema.ResolveField(tt.fieldName)
			if exists != tt.exists {
				t.Errorf("ResolveField() exists = %v, want %v", exists, tt.exists)
			}
			if !reflect.DeepEqual(got, tt.resolvedField) {
				t.Errorf("ResolveField() got = %v, want %v", got, tt.resolvedField)
			}
		})
	}
}

func TestSchema_ResolveFieldByInternalName(t *testing.T) {
	tests := []struct {
		testName  string
		schema    Schema
		fieldName string
		want      Field
		found     bool
	}{
		{
			testName:  "empty schema",
			schema:    NewSchemaWithAliases(map[FieldName]Field{}, map[FieldName]FieldName{}),
			fieldName: "message",
			want:      Field{},
			found:     false,
		},
		{
			testName:  "schema with fields with internal separators, lookup by property name",
			schema:    NewSchema(map[FieldName]Field{"foo.bar": {PropertyName: "foo.bar", InternalPropertyName: "foo::bar", Type: TypeText}}),
			fieldName: "foo.bar",
			want:      Field{},
			found:     false,
		},
		{
			testName:  "schema with fields with internal separators, lookup by internal name",
			schema:    NewSchema(map[FieldName]Field{"foo.bar": {PropertyName: "foo.bar", InternalPropertyName: "foo::bar", Type: TypeText}}),
			fieldName: "foo::bar",
			want:      Field{PropertyName: "foo.bar", InternalPropertyName: "foo::bar", Type: TypeText},
			found:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			s := NewSchemaWithAliases(tt.schema.Fields, tt.schema.Aliases)
			got, found := s.ResolveFieldByInternalName(tt.fieldName)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ResolveFieldByInternalName() got = %v, want %v", got, tt.want)
			}
			if found != tt.found {
				t.Errorf("ResolveFieldByInternalName() got1 = %v, want %v", found, tt.found)
			}
		})
	}
}
