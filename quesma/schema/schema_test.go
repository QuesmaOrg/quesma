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
			name:      "should resolve field",
			fieldName: "message",
			schema: Schema{
				Fields: map[FieldName]Field{
					"message": {Name: "message", Type: TypeText},
				},
			},
			resolvedField: Field{Name: "message", Type: TypeText},
			exists:        true,
		},
		{
			name:      "should resolve aliased field",
			fieldName: "message_alias",
			schema: Schema{
				Fields:  map[FieldName]Field{"message": {Name: "message", Type: TypeText}},
				Aliases: map[FieldName]FieldName{"message_alias": "message"},
			},
			resolvedField: Field{Name: "message", Type: TypeText},
			exists:        true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, exists := tt.schema.ResolveField(FieldName(tt.fieldName))
			if exists != tt.exists {
				t.Errorf("ResolveField() exists = %v, want %v", exists, tt.exists)
			}
			if !reflect.DeepEqual(got, tt.resolvedField) {
				t.Errorf("ResolveField() got = %v, want %v", got, tt.resolvedField)
			}
		})
	}
}
