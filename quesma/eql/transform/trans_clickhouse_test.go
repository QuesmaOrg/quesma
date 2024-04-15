package transform

import (
	"testing"
)
import "github.com/stretchr/testify/assert"

func TestExpOperatorTransformer(t *testing.T) {

	tests := []struct {
		name     string
		given    Exp
		expected Exp
	}{
		{"1", NewInfixOp("+", NewConst(1), NewConst(2)),
			NewInfixOp("+", NewConst(1), NewConst(2))},

		{"2", NewInfixOp("==", NewSymbol("foo"), NewConst(1)),
			NewInfixOp("=", NewSymbol("foo"), NewConst(1))},

		{"3", NewInfixOp("==", NewSymbol("foo"), NULL),
			NewInfixOp("IS", NewSymbol("foo"), NULL)},

		{"4", NewInfixOp("!=", TRUE, TRUE),
			NewInfixOp("<>", TRUE, TRUE)},

		{"5", NewInfixOp("and", TRUE, TRUE),
			NewInfixOp("AND", TRUE, TRUE)},

		{"6", NewInfixOp("like", NewSymbol("foo"), NewConst("bar")),
			NewInfixOp("LIKE", NewSymbol("foo"), NewConst("bar"))},

		{"7", NewInfixOp("like", NewSymbol("foo"), NewConst("*bar*")),
			NewInfixOp("LIKE", NewSymbol("foo"), NewConst("%bar%"))},

		{"8", NewInfixOp(":", NewSymbol("foo"), NewConst("bar")),
			NewInfixOp("ILIKE", NewSymbol("foo"), NewConst("bar"))},

		{"9", NewInfixOp("like", NewSymbol("foo"), NewArray(NewConst("bar"), NewConst("baz"))),
			NewInfixOp("OR",
				NewInfixOp("LIKE", NewSymbol("foo"), NewConst("bar")),
				NewInfixOp("LIKE", NewSymbol("foo"), NewConst("baz")))},

		{"10", NewInfixOp("in~", NewSymbol("foo"), NewArray(NewConst("bar"))),
			NewInfixOp("IN",
				NewFunction("lower", NewSymbol("foo")),
				NewArray(NewFunction("lower", NewConst("bar"))))},

		{"11", NewInfixOp("like~", NewSymbol("foo"), NewArray(NewConst("bar"), NewConst("baz"))),
			NewInfixOp("OR",
				NewInfixOp("ILIKE", NewSymbol("foo"), NewConst("bar")),
				NewInfixOp("ILIKE", NewSymbol("foo"), NewConst("baz")))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			trans := &ClickhouseTransformer{}

			actual := tt.given.Accept(trans)

			assert.True(t, len(trans.Errors) == 0, "errors: %", trans.Errors)
			assert.Equal(t, tt.expected, actual)
		})
	}

}
