package jsondiff

import (
	"fmt"
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
			problems: []Problem{problem("b", invalidValue)},
		},

		{
			name:     "missing value",
			expected: `{"a": 1, "b": 2, "c": 3}`,
			actual:   `{"a": 1, "c": 3}`,
			problems: []Problem{problem("b", missingValue)},
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewJSONDiff()

			problems, err := d.Diff(types.MustJSON(tt.expected), types.MustJSON(tt.actual))

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if len(problems) != len(tt.problems) {
				fmt.Println("problems:\n", problems)
				t.Errorf("Expected %d problems, got %d", len(tt.problems), len(d.problems))
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
