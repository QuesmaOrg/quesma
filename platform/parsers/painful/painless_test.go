// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painful

import (
	"github.com/QuesmaOrg/quesma/platform/util"
	"reflect"
	"testing"
)

func TestPainless(t *testing.T) {

	tests := []struct {
		name   string
		input  map[string]any
		script string
		output any
	}{
		{
			name: "simple addition",
			input: map[string]any{
				"field": 42,
			},
			script: "emit(doc['field'].value)",
			output: 42,
		},

		{
			name: "concat",
			input: map[string]any{
				"foo": "a",
				"bar": "b",
			},
			script: "emit(doc['foo'].value + doc['bar'].value)",
			output: "ab",
		},

		{
			name:   "concat strings",
			input:  map[string]any{},
			script: "emit('a' + 'b')",
			output: "ab",
		},

		{
			name: "concat date literal and string",
			input: map[string]any{
				"@timestamp": "2022-09-22T12:16:59.985Z",
				"uuid":       "1234",
			},
			script: "emit(doc['@timestamp'].value + '&' + doc['uuid'].value)",
			output: "2022-09-22T12:16:59.985Z&1234",
		},

		{
			name: "get hour from date",
			input: map[string]any{
				"@timestamp": "2022-09-22T12:16:59.98Z",
			},
			script: "emit(doc['@timestamp'].value.getHour())",
			output: 12,
		},

		{
			name: "format date with ISO",
			input: map[string]any{
				"@timestamp": "2022-09-22T12:16:59.98Z",
			},
			script: "emit(doc['@timestamp'].value.formatISO8601())",
			output: "2022-09-22T12:16:59Z",
		},

		{
			name: "url-encode",
			input: map[string]any{
				"foo": "+",
				"bar": "@",
			},
			script: "emit(URLEncoder.encode(doc['foo'].value + doc['bar'].value))",
			output: "%2B%40",
		},
	}

	for i, tt := range tests {
		t.Run(util.PrettyTestName(tt.name, i), func(t *testing.T) {
			res, err := ParsePainless(tt.script)
			if err != nil {
				t.Fatal(err)
			}

			env := &Env{
				Doc: tt.input,
			}

			switch expr := res.(type) {
			case Expr:

				_, err := expr.Eval(env)
				if err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(tt.output, env.EmitValue) {
					t.Errorf("expected %v, got %v", tt.output, env.EmitValue)
				}

			default:
				t.Fatal("not an expression")
			}
		})
	}
}
