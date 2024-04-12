package transform

import (
	"fmt"
	"testing"
)
import "github.com/stretchr/testify/assert"

func TestExpOperatorTransformer(t *testing.T) {

	tests := []struct {
		given    Exp
		expected Exp
	}{
		{NewInfixOp("+", NewConst(1), NewConst(2)),
			NewInfixOp("+", NewConst(1), NewConst(2))},

		{NewInfixOp("==", NewSymbol("foo"), NewConst(1)),
			NewInfixOp("=", NewSymbol("foo"), NewConst(1))},

		{NewInfixOp("==", NewSymbol("foo"), NULL),
			NewInfixOp("IS", NewSymbol("foo"), NULL)},

		{NewInfixOp("!=", TRUE, TRUE),
			NewInfixOp("<>", TRUE, TRUE)},

		{NewInfixOp("and", TRUE, TRUE),
			NewInfixOp("AND", TRUE, TRUE)},

		{NewInfixOp("like", NewSymbol("foo"), NewConst("bar")),
			NewInfixOp("LIKE", NewSymbol("foo"), NewConst("bar"))},

		{NewInfixOp("like", NewSymbol("foo"), NewConst("*bar*")),
			NewInfixOp("LIKE", NewSymbol("foo"), NewConst("%bar%"))},

		{NewInfixOp(":", NewSymbol("foo"), NewConst("bar")),
			NewInfixOp("ILIKE", NewSymbol("foo"), NewConst("bar"))},

		{NewInfixOp("like", NewSymbol("foo"), NewArray(NewConst("bar"), NewConst("baz"))),
			NewInfixOp("OR",
				NewInfixOp("LIKE", NewSymbol("foo"), NewConst("bar")),
				NewInfixOp("LIKE", NewSymbol("foo"), NewConst("baz")))},

		{NewInfixOp("in~", NewSymbol("foo"), NewArray(NewConst("bar"))),
			NewInfixOp("IN",
				NewFunction("lower", []Exp{NewSymbol("foo")}),
				NewArray(NewFunction("lower", []Exp{NewConst("bar")})))},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.given), func(t *testing.T) {

			trans := &InfixOpTransformer{}

			actual := tt.given.Accept(trans)

			assert.True(t, len(trans.Errors) == 0, "errors: %", trans.Errors)
			assert.Equal(t, tt.expected, actual)
		})
	}

}
