// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painful

import (
	"reflect"
	"testing"
)

func TestPainless(t *testing.T) {

	tests := []struct {
		name   string
		input  map[string]any
		script string
		output map[string]any
	}{
		{
			name: "simple addition",
			input: map[string]any{
				"field": 42,
			},
			script: "emit(doc['field'].value)",
			output: map[string]any{
				"field":     42,
				"new_field": 42,
			},
		},

		{
			name: "concat",
			input: map[string]any{
				"foo": "a",
				"bar": "b",
			},
			script: "emit(doc['foo'].value+doc['bar'].value)",
			output: map[string]any{
				"foo":       "a",
				"bar":       "b",
				"new_field": "ab",
			},
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			res, err := Parse("", []byte(tt.script))
			if err != nil {
				t.Fatal(err)
			}

			env := &Env{
				Doc:           tt.input,
				EmitFieldName: "new_field",
			}

			switch expr := res.(type) {
			case Expr:

				_, err := expr.Eval(env)
				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(tt.output, env.Doc) {
					t.Errorf("expected %v, got %v", tt.output, env.Doc)
				}

			default:
				t.Fatal("not an expression")
			}
		})
	}

}
