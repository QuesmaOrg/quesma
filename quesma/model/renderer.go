package model

import (
	"fmt"
	"strings"
)

type renderer struct{}

func RenderSQL(expr Expr) string {
	return expr.Accept(&renderer{}).(string)
}

func (v *renderer) VisitNewTableColumnExpr(e TableColumnExpr) interface{} {

	var res string

	if e.TableAlias == "" {
		res = e.ColumnName
	} else {
		res = e.TableAlias + "." + e.ColumnName
	}
	return "\"" + res + "\""
}

func (v *renderer) VisitFunction(e FunctionExpr) interface{} {
	args := make([]string, 0)
	for _, arg := range e.Args {
		args = append(args, arg.Accept(v).(string))
	}
	return e.Name + "(" + strings.Join(args, ", ") + ")"
}

func (v *renderer) VisitLiteral(l LiteralExpr) interface{} {

	if l.Value == "*" {
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

func (v *renderer) VisitString(e StringExpr) interface{} {
	return e.Value
}

func (v *renderer) VisitComposite(e CompositeExpr) interface{} {
	exps := make([]string, 0)
	for _, exp := range e.Expressions {
		exps = append(exps, exp.Accept(v).(string))
	}
	return strings.Join(exps, " ")
}

func (v *renderer) VisitSQL(s SQL) interface{} {
	return s.Query
}

func (v *renderer) VisitMultiFunction(f MultiFunctionExpr) interface{} {
	args := make([]string, 0)
	for _, arg := range f.Args {
		r := "(" + arg.Accept(v).(string) + ")"
		args = append(args, r)
	}
	return f.Name + strings.Join(args, "")
}

func (v *renderer) VisitInfix(e InfixExpr) interface{} {
	return fmt.Sprintf("%s %s %s", e.Left.Accept(v), e.Op, e.Right.Accept(v))
}
