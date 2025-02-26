// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package testdata

import (
	"github.com/stretchr/testify/assert"
	"regexp"
	"strconv"
	"testing"
)

func Test_selectFieldsInAnyOrderAsRegex(t *testing.T) {
	tests := []struct {
		args           []string
		want           string
		shouldMatch    []string
		shouldNotMatch []string
	}{
		{
			args:           []string{"a", "b", "c"},
			want:           `("a", "b", "c")|("a", "c", "b")|("b", "a", "c")|("b", "c", "a")|("c", "b", "a")|("c", "a", "b")`,
			shouldMatch:    []string{`"a", "b", "c"`, `"a", "c", "b"`, `"b", "a", "c"`, `"b", "c", "a"`, `"c", "b", "a"`, `"c", "a", "b"`},
			shouldNotMatch: []string{"a, b, c", `"a", "b"`, `a, "b", "c"`},
		},
		{
			args:           []string{"a"},
			want:           `("a")`,
			shouldMatch:    []string{`"a"`},
			shouldNotMatch: []string{"a", `"b"`},
		},
	}
	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			assert.Equal(t, tt.want, selectFieldsInAnyOrderAsRegex(tt.args))
			wantRegex := regexp.MustCompile(tt.want)
			for _, shouldMatch := range tt.shouldMatch {
				assert.True(t, wantRegex.MatchString(shouldMatch))
			}
			for _, shouldNotMatch := range tt.shouldNotMatch {
				assert.False(t, wantRegex.MatchString(shouldNotMatch))
			}
		})
	}
}
