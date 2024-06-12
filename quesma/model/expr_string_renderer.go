package model

import (
	"fmt"
	"strconv"
	"strings"
)

type renderer struct{}

// AsString renders the given expression to string which can be used to build SQL query
func AsString(expr Expr) string {
	return expr.Accept(&renderer{}).(string)
}

func (v *renderer) VisitColumnRef(e ColumnRef) interface{} {
	return strconv.Quote(strings.TrimSuffix(e.ColumnName, ".keyword"))
}

func (v *renderer) VisitPrefixExpr(e PrefixExpr) interface{} {
	args := make([]string, len(e.Args))
	for i, arg := range e.Args {
		if arg != nil {
			args[i] = arg.Accept(v).(string)
		}
	}

	argsAsString := strings.Join(args, ", ")
	return fmt.Sprintf("%v (%v)", e.Op, argsAsString)
}

func (v *renderer) VisitNestedProperty(e NestedProperty) interface{} {
	return fmt.Sprintf("%v.%v", e.ColumnRef.Accept(v), e.PropertyName.Accept(v))
}

func (v *renderer) VisitArrayAccess(e ArrayAccess) interface{} {
	return fmt.Sprintf("%v[%v]", e.ColumnRef.Accept(v), e.Index.Accept(v))
}

func (v *renderer) VisitTableColumnExpr(e TableColumnExpr) interface{} {

	var res string

	if e.TableAlias == "" {
		res = v.VisitColumnRef(e.ColumnRef).(string)
	} else {
		res = e.TableAlias + "." + v.VisitColumnRef(e.ColumnRef).(string)
	}
	return res
}

func (v *renderer) VisitFunction(e FunctionExpr) interface{} {
	args := make([]string, 0)
	for _, arg := range e.Args {
		args = append(args, arg.Accept(v).(string))
	}
	return e.Name + "(" + strings.Join(args, ",") + ")"
}

func (v *renderer) VisitLiteral(l LiteralExpr) interface{} {

	if l.Value == "*" {
		return "*"
	}

	switch l.Value.(type) {
	case string:
		return fmt.Sprintf("%s", l.Value)
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
	var lhs, rhs interface{} // TODO FOR NOW LITTLE PARANOID BUT HELPS ME NOT SEE MANY PANICS WHEN TESTING
	if e.Left != nil {
		lhs = e.Left.Accept(v)
	} else {
		lhs = "< LHS NIL >"
	}
	if e.Right != nil {
		rhs = e.Right.Accept(v)
	} else {
		rhs = "< RHS NIL >"
	}
	// This might look like a strange heuristics to but is aligned with the way we are currently generating the statement
	// I think in the future every infix op should be in braces.
	if e.Op == "AND" || e.Op == "OR" {
		return fmt.Sprintf("(%v %v %v)", lhs, e.Op, rhs)
	} else if strings.Contains(e.Op, "LIKE") || e.Op == "IS" || e.Op == "IN" {
		return fmt.Sprintf("%v %v %v", lhs, e.Op, rhs)
	} else {
		return fmt.Sprintf("%v%v%v", lhs, e.Op, rhs)
	}
}

func (v *renderer) VisitOrderByExpr(e OrderByExpr) interface{} {
	var exprsAsStr []string
	for _, expr := range e.Exprs {
		exprsAsStr = append(exprsAsStr, expr.Accept(v).(string))
	}
	allExprs := strings.Join(exprsAsStr, ", ")
	if e.Direction == DescOrder {
		return fmt.Sprintf("%s %s", allExprs, "DESC")
	}
	if e.Direction == AscOrder {
		return fmt.Sprintf("%s %s", allExprs, "ASC")
	}
	return allExprs
}
