package aexp

import (
	"fmt"
	"strings"
)

type renderer struct{}

func (v *renderer) VisitTableColumn(e TableColumnExp) interface{} {

	var res string

	if e.TableAlias == "" {
		res = e.ColumnName
	} else {
		res = e.TableAlias + "." + e.ColumnName
	}
	return "\"" + res + "\""
}

func (v *renderer) VisitFunction(e FunctionExp) interface{} {
	args := make([]string, 0)
	for _, arg := range e.Args {
		args = append(args, arg.Accept(v).(string))
	}
	return e.Name + "(" + strings.Join(args, ", ") + ")"
}

func (v *renderer) VisitLiteral(l LiteralExp) interface{} {

	if l == Wildcard {
		return "*"
	}

	switch l.Value.(type) {
	case string:
		return fmt.Sprintf("'%s'", l.Value)
	case float64:
		return fmt.Sprintf("%f", l.Value)
	default:
		return fmt.Sprintf("%v", l.Value)
	}
}

func (v *renderer) VisitString(e StringExp) interface{} {
	return e.Value
}

func (v *renderer) VisitComposite(e CompositeExp) interface{} {
	exps := make([]string, 0)
	for _, exp := range e.Expressions {
		exps = append(exps, exp.Accept(v).(string))
	}
	return strings.Join(exps, " ")
}

func (v *renderer) VisitSQL(s SQL) interface{} {
	return s.Query
}

func (v *renderer) VisitMultiFunction(f MultiFunctionExp) interface{} {
	args := make([]string, 0)
	for _, arg := range f.Args {
		r := "(" + arg.Accept(v).(string) + ")"
		args = append(args, r)
	}
	return f.Name + strings.Join(args, "")
}

func (v *renderer) VisitInfix(e InfixExp) interface{} {
	return fmt.Sprintf("%s %s %s", e.Left.Accept(v), e.Op, e.Right.Accept(v))
}
