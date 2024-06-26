// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package transform

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExpMap(t *testing.T) {

	in := []Exp{&Const{Value: 1}, &Const{Value: 2}}

	actual := mapExp(func(e Exp) Exp {
		return &Const{Value: e.(*Const).Value.(int) * 2}
	}, in)

	expected := []Exp{&Const{Value: 2}, &Const{Value: 4}}

	assert.Equal(t, expected, actual)
}

func TestExpReduce(t *testing.T) {
	in := []Exp{&Const{Value: 1}, &Const{Value: 2}}

	actual := reduceExp(func(left Exp, right Exp) Exp {
		return &InfixOp{
			Op:    "+",
			Left:  left,
			Right: right,
		}
	}, in)

	expected := &InfixOp{
		Op:    "+",
		Left:  &Const{Value: 1},
		Right: &Const{Value: 2},
	}

	assert.Equal(t, expected, actual)
}

func TestExpReduce2(t *testing.T) {
	in := []Exp{&Const{Value: 1}, &Const{Value: 2}, &Const{Value: 3}}

	actual := reduceExp(func(left Exp, right Exp) Exp {
		return &InfixOp{
			Op:    "+",
			Left:  left,
			Right: right,
		}
	}, in)

	expected := &InfixOp{
		Op:   "+",
		Left: &Const{Value: 1},
		Right: &InfixOp{
			Op:    "+",
			Left:  &Const{Value: 2},
			Right: &Const{Value: 3},
		},
	}

	assert.Equal(t, expected, actual)
}

func TestExpToORs(t *testing.T) {

	in := []Exp{&Const{Value: 1}, &Const{Value: 2}, &Const{Value: 3}}

	actual := mapReduceToORExpressions(func(e Exp) Exp {
		return &Function{Name: Symbol{Name: "test"}, Args: []Exp{e}}
	}, in)

	expected := &InfixOp{
		Op:   "OR",
		Left: &Function{Name: Symbol{Name: "test"}, Args: []Exp{&Const{Value: 1}}},
		Right: &InfixOp{
			Op:    "OR",
			Left:  &Function{Name: Symbol{Name: "test"}, Args: []Exp{&Const{Value: 2}}},
			Right: &Function{Name: Symbol{Name: "test"}, Args: []Exp{&Const{Value: 3}}},
		},
	}

	assert.Equal(t, expected, actual)

}
