package where_clause

import (
	"fmt"
	"strconv"
	"strings"
)

// StringRenderer is a visitor that renders the WHERE statement as a string
type StringRenderer struct {
}

func (v *StringRenderer) VisitLiteral(e *Literal) interface{} {
	return e.Name
}

func (v *StringRenderer) VisitInfixOp(e *InfixOp) interface{} {
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

func (v *StringRenderer) VisitPrefixOp(e *PrefixOp) interface{} {
	args := make([]string, len(e.Args))
	for i, arg := range e.Args {
		if arg != nil {
			args[i] = arg.Accept(v).(string)
		}
	}

	argsAsString := strings.Join(args, ", ")
	return fmt.Sprintf("%v (%v)", e.Op, argsAsString)
}

func (v *StringRenderer) VisitFunction(e *Function) interface{} {
	args := make([]string, len(e.Args))
	for i, arg := range e.Args {
		if arg != nil {
			args[i] = arg.Accept(v).(string)
		}
	}

	argsAsString := strings.Join(args, ",")
	return fmt.Sprintf("%v(%v)", e.Name.Accept(v), argsAsString)
}

func (v *StringRenderer) VisitColumnRef(e *ColumnRef) interface{} {
	return strconv.Quote(e.ColumnName)
}

func (v *StringRenderer) VisitNestedProperty(e *NestedProperty) interface{} {
	return fmt.Sprintf("%v.%v", e.ColumnRef.Accept(v), e.PropertyName.Accept(v))
}

func (v *StringRenderer) VisitArrayAccess(e *ArrayAccess) interface{} {
	return fmt.Sprintf("%v[%v]", e.ColumnRef.Accept(v), e.Index.Accept(v))
}
