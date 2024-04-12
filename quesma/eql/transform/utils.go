package transform

func mapExp(fn func(Exp) Exp, array *Array) []Exp {
	var values []Exp
	for _, value := range array.Values {
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
func mapReduceToORExpressions(fn func(Exp) Exp, array *Array) Exp {
	return reduceExp(func(left, right Exp) Exp {
		return &InfixOp{
			Op:    "OR",
			Left:  left,
			Right: right,
		}
	}, mapExp(fn, array))
}

func IsNULL(e Exp) bool {
	s, ok := e.(*Symbol)
	return ok && s == NULL
}
