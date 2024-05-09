package transform

import (
	"fmt"
	"strings"
)

func clickhouseRaiseError(msg string, args ...interface{}) Exp {
	return NewFunction("throwIf", TRUE, NewConst(fmt.Sprintf(msg, args...)))
}

//
// Converts EQL expressions to Clickhouse expressions
// It converts:
// 1. operators
// 2. functions

type ClickhouseTransformer struct {
	Errors []string
}

func (t *ClickhouseTransformer) error(msg string, args ...interface{}) Exp {
	t.Errors = append(t.Errors, fmt.Sprintf(msg, args...))

	// this is paranoid
	// if some else ignores the error, we return expression that will throw an error
	return clickhouseRaiseError(msg, args...)

}

func (t *ClickhouseTransformer) visitChildren(c []Exp) []Exp {
	var result []Exp
	for _, child := range c {
		result = append(result, child.Accept(t).(Exp))
	}
	return result
}

func (t *ClickhouseTransformer) VisitConst(e *Const) interface{} {
	return e
}

func (t *ClickhouseTransformer) VisitSymbol(e *Symbol) interface{} {
	return e
}

func (t *ClickhouseTransformer) VisitGroup(e *Group) interface{} {
	e.Inner = e.Inner.Accept(t).(Exp)
	return e
}

func (t *ClickhouseTransformer) replaceConstLikePattern(exp Exp) Exp {

	if constant, ok := exp.(*Const); ok {
		if s, ok := constant.Value.(string); ok {

			// Fist escape % nad _, because it's a special character in LIKE operator

			s = strings.ReplaceAll(s, "%", "\\%")
			s = strings.ReplaceAll(s, "_", "\\_")

			s = strings.ReplaceAll(s, "*", "%") // replace * with % for LIKE operator
			s = strings.ReplaceAll(s, "?", "_") // replace ? with _ for LIKE operator

			return NewConst(s)

		}
	}
	// if it's not a string, return the original expression
	return exp
}

func (t *ClickhouseTransformer) replaceLikePattern(list []Exp) []Exp {
	return mapExp(t.replaceConstLikePattern, list)
}

func (t *ClickhouseTransformer) clickhouseLower(e Exp) Exp {
	return NewFunction("lower", e)
}

func (t *ClickhouseTransformer) VisitInfixOp(e *InfixOp) interface{} {
	left := e.Left.Accept(t).(Exp)
	right := e.Right.Accept(t).(Exp)

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

	case "not in~", "in~":

		targetOp := "IN"
		if op == "not in~" {
			targetOp = "NOT IN"
		}

		if array, ok := right.(*Array); ok {

			fn := func(e Exp) Exp {
				return t.clickhouseLower(e)
			}

			return NewInfixOp(targetOp,
				t.clickhouseLower(left),
				NewArray(mapExp(fn, array.Values)...))
		}

		return t.error(op + " operator requires a list of values")

	case "like":

		if array, ok := right.(*Array); ok {

			fn := func(e Exp) Exp {
				return NewInfixOp("LIKE", left, e)
			}

			return mapReduceToORExpressions(fn, t.replaceLikePattern(array.Values))
		}
		return NewInfixOp("LIKE", left, t.replaceConstLikePattern(right))

	case "like~", ":":

		if array, ok := right.(*Array); ok {
			fn := func(e Exp) Exp {
				return NewInfixOp("ILIKE", left, e)
			}

			return mapReduceToORExpressions(fn, t.replaceLikePattern(array.Values))
		}
		return NewInfixOp("ILIKE", left, t.replaceConstLikePattern(right))

	case "regex", "regex~":

		if array, ok := right.(*Array); ok {
			fn := func(e Exp) Exp {
				return NewFunction("match", left, e)
			}

			return mapReduceToORExpressions(fn, array.Values)
		} else {
			return NewFunction("match", left, right)
		}

	default:
		return t.error("Unknown infix operator: %s", op)
	}

}

func (t *ClickhouseTransformer) VisitPrefixOp(e *PrefixOp) interface{} {

	op := e.Op
	switch op {

	case "not":
		op = "NOT"

	default:
		t.Errors = append(t.Errors, fmt.Sprintf("Unknown prefix operator: %s ", op))
	}

	return NewPrefixOp(op, t.visitChildren(e.Args))
}

func (t *ClickhouseTransformer) funcArityError(funcName string, expected string, actual int) Exp {
	return t.error("'%s' function requires %s argument(s), but got %d", funcName, expected, actual)
}

func (t *ClickhouseTransformer) VisitFunction(e *Function) interface{} {

	name := e.Name.Name
	args := t.visitChildren(e.Args)
	argsCount := len(args)

	switch name {

	case "add":
		if argsCount != 2 {
			return t.funcArityError(name, "2", argsCount)
		}

		return NewInfixOp("+", args[0], args[1])

	case "between":
		if argsCount != 3 {
			return t.funcArityError(name, "3", argsCount)
		}

		return t.error("between function not implemented")

	case "cidrMatch":
		//https://clickhouse.com/docs/en/sql-reference/functions/ip-address-functions#isipaddressinrange

		if argsCount < 2 {
			return t.funcArityError(name, "at least 2", argsCount)
		}

		ipAddress := e.Args[0]

		fn := func(e Exp) Exp {
			return NewFunction("isIPAddressInRange", ipAddress, e)
		}
		return mapReduceToORExpressions(fn, args[1:])

	case "concat":
		if argsCount < 1 {
			return t.funcArityError(name, "at least 1", argsCount)
		}
		return NewFunction("concat", t.visitChildren(e.Args)...)

	case "divide":
		if argsCount != 2 {
			return t.funcArityError(name, "2", argsCount)
		}

		return NewInfixOp("/", args[0], args[1])

	case "endsWith~", "endwith~":
		if argsCount != 2 {
			return t.funcArityError(name, "2", argsCount)
		}

		return NewFunction("endsWithUTF8", t.clickhouseLower(args[0]), t.clickhouseLower(args[1]))

	case "endsWith", "endswith~":
		if argsCount != 2 {
			return t.funcArityError(name, "2", argsCount)
		}

		return NewFunction("endsWithUTF8", args[0], args[1])

	case "indexOf":
		if argsCount != 2 && argsCount != 3 {
			return t.funcArityError(name, "2 or 3", argsCount)
		}

		return NewFunction("position", args...)

	case "indexOf~":
		if argsCount != 2 && argsCount != 3 {
			return t.funcArityError(name, "2 or 3", argsCount)
		}

		args[0] = t.clickhouseLower(args[0])
		args[1] = t.clickhouseLower(args[1])

		return NewFunction("position", args...)

	case "length":
		if argsCount != 1 {
			return t.funcArityError(name, "1", argsCount)
		}

		return NewFunction("length", args[0])

	case "modulo":
		if argsCount != 2 {
			return t.funcArityError(name, "2", argsCount)
		}

		return NewInfixOp("%", args[0], args[1])

	case "multiply":
		if argsCount != 2 {
			return t.funcArityError(name, "2", argsCount)
		}

		return NewInfixOp("*", args[0], args[1])

	case "number":
		if argsCount != 1 {
			return t.funcArityError(name, "1", argsCount)
		}

		return NewFunction("toFloat", args[0])

	case "startsWith", "startswith": // examples have both spelling
		if argsCount != 2 {
			return t.funcArityError(name, "2", argsCount)
		}

		return NewFunction("startsWithUTF8", args[0], args[1])

	case "startsWith~", "startswith~":
		if argsCount != 2 {
			return t.funcArityError(name, "2", argsCount)
		}

		return NewFunction("startsWithUTF8",
			t.clickhouseLower(args[0]), t.clickhouseLower(args[1]))

	case "string":
		if argsCount != 1 {
			return t.funcArityError(name, "1", argsCount)
		}

		return NewFunction("toString", args[0])

	case "stringContains":
		if argsCount != 2 {
			return t.funcArityError(name, "2", argsCount)
		}

		return NewFunction("hasSubsequence", args...)

	case "stringContains~":
		if argsCount != 2 {
			return t.funcArityError(name, "2", argsCount)
		}

		return NewFunction("hasSubsequenceCaseInsensitive", args...)

	case "substring":
		if argsCount != 3 && argsCount != 2 {
			return t.funcArityError(name, "2 or 3", argsCount)
		}

		return NewFunction("substring", args...)

	case "subtract":
		if argsCount != 2 {
			return t.funcArityError(name, "2", argsCount)
		}

		return NewInfixOp("-", args[0], args[1])

	default:
		return t.error("Unknown EQL function '%s'", name)
	}

}

func (t *ClickhouseTransformer) VisitArray(e *Array) interface{} {
	e.Values = t.visitChildren(e.Values)
	return e
}
