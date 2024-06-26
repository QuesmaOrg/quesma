// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package transform

func mapExp(fn func(Exp) Exp, list []Exp) []Exp {
	var values []Exp
	for _, value := range list {
		values = append(values, fn(value))
	}
	return values
}

func reduceExp(fn func(Exp, Exp) Exp, exps []Exp) Exp {
	if len(exps) == 1 {
		return exps[0]
	}

	return fn(exps[0], reduceExp(fn, exps[1:]))
}

// this used to convert
// "foo like (1,2)"  -> "((foo like 1) or (foo like 2))
func mapReduceToORExpressions(fn func(Exp) Exp, list []Exp) Exp {
	return reduceExp(func(left, right Exp) Exp {
		return &InfixOp{
			Op:    "OR",
			Left:  left,
			Right: right,
		}
	}, mapExp(fn, list))
}

func IsNULL(e Exp) bool {
	s, ok := e.(*Symbol)
	return ok && s == NULL
}
