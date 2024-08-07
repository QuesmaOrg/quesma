// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package jsondiff

import (
	"fmt"
	"github.com/k0kubun/pp"
	"quesma/quesma/types"
	"testing"
)

func TestJSONDiff(t *testing.T) {

	problem := func(path string, problemType problemType) Problem {
		return Problem{
			Path: path,
			Type: problemType.code,
		}
	}

	tests := []struct {
		name     string
		expected string
		actual   string
		problems []Problem
	}{
		{
			name:     "Test 1",
			expected: `{"a": 1, "b": 2, "c": 3}`,
			actual:   `{"a": 1, "b": 2, "c": 3}`,
			problems: []Problem{},
		},

		{
			name:     "Test 2",
			expected: `{"a": 1, "b": 2, "c": 3}`,
			actual:   `{"a": 1, "b": 3, "c": 3}`,
			problems: []Problem{problem("b", invalidValue)},
		},

		{
			name:     "invalid type",
			expected: `{"a": 1, "b": 2, "c": 3}`,
			actual:   `{"a": 1, "b": "foo", "c": 3}`,
			problems: []Problem{problem("b", invalidType)},
		},

		{
			name:     "missing value",
			expected: `{"a": 1, "b": 2, "c": 3}`,
			actual:   `{"a": 1, "c": 3}`,
			problems: []Problem{problem("b", invalidValue)},
		},

		{
			name:     "array length",
			expected: `{"a": [1, 2, 3], "b": 2, "c": 3}`,
			actual:   `{"a": [1, 2], "b": 2, "c": 3}`,
			problems: []Problem{problem("a", invalidArrayLength)},
		},

		{
			name:     "array element difference",
			expected: `{"a": [1, 2, 3], "b": 2, "c": 3}`,
			actual:   `{"a": [1, 2, 4], "b": 2, "c": 3}`,
			problems: []Problem{problem("a.[2]", invalidValue)},
		},

		{
			name:     "array element difference",
			expected: `{"a": [1, 2, 3]}`,
			actual:   `{"a": [1, true, "xx"]}`,
			problems: []Problem{problem("a.[1]", invalidType), problem("a.[2]", invalidType)},
		},

		{
			name:     "object difference",
			expected: `{"a": {"b": 1}, "c": 3}`,
			actual:   `{"a": {"b": 2}, "c": 3}`,
			problems: []Problem{problem("a.b", invalidValue)},
		},

		{
			name:     "deep path difference",
			expected: `{"a": {"d": {"b": 1}}, "c": 3}`,
			actual:   `{"a": {"d": {"b": 2}}, "c": 3}`,
			problems: []Problem{problem("a.d.b", invalidValue)},
		},

		{
			name:     "deep path difference",
			expected: `{"a": {"d": {"b": 1}}, "c": 3, "_ignore": 1}`,
			actual:   `{"a": {"d": {"b": 2}}, "c": 3}`,
			problems: []Problem{problem("a.d.b", invalidValue)},
		},

		{
			name:     "array sort difference ",
			expected: `{"a": [1, 2, 3], "b": 2, "c": 3}`,
			actual:   `{"a": [1, 3, 2], "b": 2, "c": 3}`,
			problems: []Problem{problem("a.[1]", invalidValue), problem("a.[2]", invalidValue)},
		},

		{
			name:     "array sort difference (with key extractor)",
			expected: `{"bar": [5, 2, 3], "b": 2, "c": 3}`,
			actual:   `{"bar": [5, 3, 2], "b": 2, "c": 3}`,
			problems: []Problem{problem("bar", arraySortDifference)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			diff, err := NewJSONDiff("_ignore")
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			err = diff.AddKeyExtractor("bar", func(v any) (string, error) {
				return fmt.Sprintf("%v", v), nil
			})

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			problems, err := diff.Diff(types.MustJSON(tt.expected), types.MustJSON(tt.actual))

			pp.Println("problems:\n", problems)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if len(problems) != len(tt.problems) {
				fmt.Println("problems:\n", problems)
				t.Errorf("Expected %d problems, got %d", len(tt.problems), len(diff.problems))
			}

			for i, p := range problems {
				if p.Path != tt.problems[i].Path {
					t.Errorf("Expected path %s, got %s", tt.problems[i].Path, p.Path)
				}
				if p.Type != tt.problems[i].Type {
					t.Errorf("Expected type %s, got %s", tt.problems[i].Type, p.Type)
				}
			}
		})
	}
}
