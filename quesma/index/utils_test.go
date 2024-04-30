package index

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTableNamePatternRegexp(t *testing.T) {
	tests := []struct {
		input  string
		output string
	}{
		{input: "foo", output: "^foo$"},
		{input: "foo*", output: "^foo.*$"},
		{input: "foo*bar", output: "^foo.*bar$"},
		{input: "foo*bar*", output: "^foo.*bar.*$"},
		{input: "foo*b[ar*", output: "^foo.*b\\[ar.*$"},
		{input: "foo+bar", output: "^foo\\+bar$"},
		{input: "foo|bar", output: "^foo\\|bar$"},
		{input: "foo(bar", output: "^foo\\(bar$"},
		{input: "foo)bar", output: "^foo\\)bar$"},
		{input: "foo^bar", output: "^foo\\^bar$"},
		{input: "foo$bar", output: "^foo\\$bar$"},
		{input: "foo.bar", output: "^foo\\.bar$"},
		{input: "foo\\bar", output: "^foo\\\\bar$"},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s into %s", tt.input, tt.output), func(t *testing.T) {
			if got := TableNamePatternRegexp(tt.input); !reflect.DeepEqual(got.String(), tt.output) {
				t.Errorf("TableNamePatternRegexp() = %v, want %v", got, tt.output)
			}
		})
	}
}
