// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package jsondiff

import (
	"fmt"
	"strings"

	"github.com/k0kubun/pp"

	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"testing"
)

func TestJSONDiff(t *testing.T) {

	var debug = false

	mismatch := func(path string, mType mismatchType) JSONMismatch {
		return JSONMismatch{
			Path: path,
			Type: mType.code,
		}
	}

	tests := []struct {
		name     string
		expected string
		actual   string
		problems []JSONMismatch
	}{
		{
			name:     "Test 1",
			expected: `{"a": 1, "b": 2, "c": 3}`,
			actual:   `{"a": 1, "b": 2, "c": 3}`,
			problems: []JSONMismatch{},
		},

		{
			name:     "Test 2",
			expected: `{"a": 1, "b": 2, "c": 3}`,
			actual:   `{"a": 1, "b": 3, "c": 3}`,
			problems: []JSONMismatch{mismatch("b", invalidNumberValue)},
		},

		{
			name:     "invalid type",
			expected: `{"a": 1, "b": 2, "c": 3}`,
			actual:   `{"a": 1, "b": "foo", "c": 3}`,
			problems: []JSONMismatch{mismatch("b", invalidType)},
		},

		{
			name:     "missing value",
			expected: `{"a": 1, "b": 2, "c": 3}`,
			actual:   `{"a": 1, "c": 3}`,
			problems: []JSONMismatch{mismatch("b", invalidValue)},
		},

		{
			name:     "array length",
			expected: `{"a": [1, 2, 3], "b": 2, "c": 3}`,
			actual:   `{"a": [1, 2], "b": 2, "c": 3}`,
			problems: []JSONMismatch{mismatch("a", invalidArrayLengthOffByOne)},
		},

		{
			name:     "array element difference",
			expected: `{"a": [1, 2, 3], "b": 2, "c": 3}`,
			actual:   `{"a": [1, 2, 4], "b": 2, "c": 3}`,
			problems: []JSONMismatch{mismatch("a.[2]", invalidNumberValue)},
		},

		{
			name:     "array element difference",
			expected: `{"a": [1, 2, 3]}`,
			actual:   `{"a": [1, true, "xx"]}`,
			problems: []JSONMismatch{mismatch("a.[1]", invalidType), mismatch("a.[2]", invalidType)},
		},

		{
			name:     "object difference",
			expected: `{"a": {"b": 1}, "c": 3}`,
			actual:   `{"a": {"b": 2}, "c": 3}`,
			problems: []JSONMismatch{mismatch("a.b", invalidNumberValue)},
		},

		{
			name:     "deep path difference",
			expected: `{"a": {"d": {"b": 1}}, "c": 3}`,
			actual:   `{"a": {"d": {"b": 2}}, "c": 3}`,
			problems: []JSONMismatch{mismatch("a.d.b", invalidNumberValue)},
		},

		{
			name:     "deep path difference",
			expected: `{"a": {"d": {"b": 1}}, "c": 3, "_ignore": 1}`,
			actual:   `{"a": {"d": {"b": 2}}, "c": 3}`,
			problems: []JSONMismatch{mismatch("a.d.b", invalidNumberValue)},
		},

		{
			name:     "array sort difference ",
			expected: `{"a": [1, 2, 3], "b": 2, "c": 3}`,
			actual:   `{"a": [1, 3, 2], "b": 2, "c": 3}`,
			problems: []JSONMismatch{mismatch("a.[1]", invalidNumberValue), mismatch("a.[2]", invalidNumberValue)},
		},

		{
			name:     "array sort difference (with key extractor)",
			expected: `{"bar": [5, 2, 3], "b": 2, "c": 3}`,
			actual:   `{"bar": [5, 3, 2], "b": 2, "c": 3}`,
			problems: []JSONMismatch{mismatch("bar", arrayKeysSortDifference)},
		},

		{
			name:     "array sort difference ",
			expected: `{"bar": [5, 2, 4, 3, 1, 0], "b": 2, "c": 3}`,
			actual:   `{"bar": [5, 2, 4, 3, 1, -1], "b": 2, "c": 3}`,
			problems: []JSONMismatch{mismatch("bar", arrayKeysDifferenceSlightly)},
		},
		{
			name:     "dates",
			expected: `{"a": "2021-01-01T00:00:00.000Z"}`,
			actual:   `{"a": "2021-01-01T00:00:00.001Z"}`,
			problems: []JSONMismatch{mismatch("a", invalidDateValue)},
		},
		{
			name:     "dates 2",
			expected: `{"a": "2021-01-01T00:00:00.000Z"}`,
			actual:   `{"a": "2021-01-01T00:00:00.000"}`,
			problems: []JSONMismatch{},
		},
		{
			name:     "dates 3",
			expected: `{"a": "2024-10-24T10:00:00.000"}`,
			actual:   `{"a": "2024-10-24T12:00:00.000+02:00"}`,
		},
		{
			name:     "dates 4",
			expected: `{"a": "2024-10-31T11:00:00.000"}`,
			actual:   `{"a": "2024-10-31T12:00:00.000+01:00"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if strings.HasPrefix(tt.name, "SKIP") {
				return
			}

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

			if debug {
				pp.Println("problems:\n", problems)
			}
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if len(problems) != len(tt.problems) {
				fmt.Println("problems:\n", problems)
				t.Errorf("Expected %d problems, got %d", len(tt.problems), len(diff.mismatches))
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
