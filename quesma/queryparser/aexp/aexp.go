package aexp

import (
	"fmt"
	"strings"
)

type AExp interface {
	Accept(v AExpVisitor) interface{}
}

func RenderSQL(exp AExp) string {
	return exp.Accept(&renderer{}).(string)
}

type TableColumn struct {
	TableName  string
	ColumnName string
}

func (e TableColumn) String() string {
	return fmt.Sprintf("(tablecolumn '%s' . '%s') ", e.TableName, e.ColumnName)
}

func C(columnName string) TableColumn {

	if strings.HasSuffix(columnName, ".keyword") {
		columnName = strings.TrimSuffix(columnName, ".keyword")
	}

	return TableColumn{ColumnName: columnName}
}

func (e TableColumn) Accept(v AExpVisitor) interface{} {
	return v.VisitTableColumn(e)
}

type Function struct {
	Name string
	Args []AExp
}

func (e Function) Accept(v AExpVisitor) interface{} {
	return v.VisitFunction(e)
}

func FN(name string, args ...AExp) Function {
	return Function{Name: name, Args: args}
}

func Count(args ...AExp) Function {
	return FN("count", args...)
}

// It represents functions with multitple arguments list
// like `quantile(level)(expr)
type MultiFunction struct {
	Name string
	Args []AExp
}

func (e MultiFunction) Accept(v AExpVisitor) interface{} {
	return v.VisitMultiFunction(e)
}

type Literal struct {
	Value any
}

func (e Literal) String() string {
	return fmt.Sprintf("(literal %s)", e.Value)
}

func L(value any) Literal {
	return Literal{Value: value}
}

var Wildcard = Literal{Value: "*"}

// it will render as IS
type symbol string

func Symbol(s string) Literal {
	return L(symbol(s))
}

func (e Literal) Accept(v AExpVisitor) interface{} {
	return v.VisitLiteral(e)
}

// Space separated expressions
type Composite struct {
	Expressions []AExp
}

func (e Composite) Accept(v AExpVisitor) interface{} {
	return v.VisitComposite(e)
}

func NewComposite(expressions ...AExp) *Composite {
	return &Composite{Expressions: expressions}
}

// ASIS expressions, this is workaroung for not supported expressions
// It can be named as TODO.
type SQL struct {
	Query string
}

func (s SQL) Accept(v AExpVisitor) interface{} {
	return v.VisitSQL(s)
}

type AExpVisitor interface {
	VisitTableColumn(e TableColumn) interface{}
	VisitFunction(e Function) interface{}
	VisitMultiFunction(e MultiFunction) interface{}
	VisitLiteral(l Literal) interface{}
	VisitComposite(e Composite) interface{}
	VisitSQL(s SQL) interface{}
}

type renderer struct{}

func (v *renderer) VisitTableColumn(e TableColumn) interface{} {

	var res string

	if e.TableName == "" {
		res = e.ColumnName
	} else {
		res = e.TableName + "." + e.ColumnName
	}
	return "\"" + res + "\""
}

func (v *renderer) VisitFunction(e Function) interface{} {
	args := make([]string, 0)
	for _, arg := range e.Args {
		args = append(args, arg.Accept(v).(string))
	}
	return e.Name + "(" + strings.Join(args, ", ") + ")"

}

func (v *renderer) VisitLiteral(l Literal) interface{} {

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

func (v *renderer) VisitComposite(e Composite) interface{} {
	exps := make([]string, 0)
	for _, exp := range e.Expressions {
		exps = append(exps, exp.Accept(v).(string))
	}
	return strings.Join(exps, " ")
}

func (v *renderer) VisitSQL(s SQL) interface{} {
	return s.Query
}

func (v *renderer) VisitMultiFunction(f MultiFunction) interface{} {
	args := make([]string, 0)
	for _, arg := range f.Args {
		r := "(" + arg.Accept(v).(string) + ")"
		args = append(args, r)
	}
	return f.Name + strings.Join(args, "")
}
