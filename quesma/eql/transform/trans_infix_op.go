package transform

import (
	"fmt"
	"strings"
)

type InfixOpTransformer struct {
	Errors []string
}

func (v *InfixOpTransformer) visitChildren(c []Exp) []Exp {
	var result []Exp
	for _, child := range c {
		result = append(result, child.Accept(v).(Exp))
	}
	return result
}

func (v *InfixOpTransformer) VisitConst(e *Const) interface{} {
	return e
}

func (v *InfixOpTransformer) VisitSymbol(e *Symbol) interface{} {
	return e
}

func (v *InfixOpTransformer) VisitGroup(e *Group) interface{} {
	e.Inner = e.Inner.Accept(v).(Exp)
	return e
}

func (v *InfixOpTransformer) replaceConstLikePattern(exp Exp) Exp {

	if constant, ok := exp.(*Const); ok {
		if s, ok := constant.Value.(string); ok {
			return NewConst(strings.Replace(s, "*", "%", -1))
		}
	}
	// if it's not a string, return the original expression
	return exp
}

func (v *InfixOpTransformer) replaceLikePattern(array *Array) *Array {
	return NewArray(mapExp(v.replaceConstLikePattern, array)...)
}

func (v *InfixOpTransformer) VisitInfixOp(e *InfixOp) interface{} {
	left := e.Left.Accept(v).(Exp)
	right := e.Right.Accept(v).(Exp)

	op := e.Op
	switch op {

	case "and", "or", "in", "not in":
		op = strings.ToUpper(op)
		return NewInfixOp(op, left, right)

	case ">", "<", ">=", "<=":
		return NewInfixOp(op, left, right)

	case "+", "*", "-", "/", "%":
		return NewInfixOp(op, left, right)

	case "==":
		op = "="

		if IsNULL(right) {
			op = "IS"
		}
		return NewInfixOp(op, left, right)

	case "!=":

		if IsNULL(right) {
			op = "IS NOT"
		} else {
			op = "<>"
		}

		return NewInfixOp(op, left, right)

	case "in~":

		if array, ok := right.(*Array); ok {

			fn := func(e Exp) Exp {
				return NewFunction("lower", []Exp{e})
			}
			return NewInfixOp("IN",
				NewFunction("lower", []Exp{left}),
				NewArray(mapExp(fn, array)...))
		}

		return NewInfixOp("IN", NewFunction("lower", []Exp{left}), right)

	case "like":

		if _, ok := right.(*Array); ok {

			fn := func(e Exp) Exp {
				return NewInfixOp("LIKE", left, e)
			}
			return mapReduceToORExpressions(fn, v.replaceLikePattern(right.(*Array)))
		}
		return NewInfixOp("LIKE", left, v.replaceConstLikePattern(right))

	case "like~", ":":

		if _, ok := right.(*Array); ok {
			fn := func(e Exp) Exp {
				return NewInfixOp("ILIKE", left, e)
			}
			return mapReduceToORExpressions(fn, v.replaceLikePattern(right.(*Array)))
		}
		return NewInfixOp("ILIKE", left, v.replaceConstLikePattern(right))

	case "regex", "regex~":

		if _, ok := right.(*Array); ok {
			fn := func(e Exp) Exp {
				return NewFunction("match", []Exp{left, e})
			}
			return mapReduceToORExpressions(fn, right.(*Array))
		} else {
			return NewFunction("match", []Exp{left, right})
		}

	default:
		v.Errors = append(v.Errors, fmt.Sprintf("Unknown infix operator: %s ", op))
		// FIXME add a 'broken' Exp here
		return TRUE
	}

}

func (v *InfixOpTransformer) VisitPrefixOp(e *PrefixOp) interface{} {

	op := e.Op
	switch op {

	case "not":
		op = "NOT"

	default:
		v.Errors = append(v.Errors, fmt.Sprintf("Unknown prefix operator: %s ", op))
	}

	return NewPrefixOp(op, v.visitChildren(e.Args))
}

func (v *InfixOpTransformer) VisitFunction(e *Function) interface{} {
	e.Args = v.visitChildren(e.Args)
	return e
}

func (v *InfixOpTransformer) VisitArray(e *Array) interface{} {
	e.Values = v.visitChildren(e.Values)
	return e
}
