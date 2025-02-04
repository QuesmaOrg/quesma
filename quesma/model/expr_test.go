// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParenExpr(t *testing.T) {
	parenExpr := NewInfixExpr(NewParenExpr(
		NewInfixExpr(
			NewFunction("floor", NewLiteral(1.5)),
			"+", NewLiteral(2.5))), "/", NewLiteral(3.5))
	assert.Equal(t, "(floor(1.5)+2.5)/3.5", AsString(parenExpr))
}

func Test_escapeString(t *testing.T) {
	testcases := []struct {
		input  string
		output string
	}{
		{``, ``},
		{`abc`, `abc`},
		{`a'bc`, `a\'bc`},
		{`a\bc`, `a\\bc`},
		{`a%bc`, `a\%bc`},
	}
	for _, tc := range testcases {
		assert.Equal(t, tc.output, escapeString(tc.input))
	}
}
