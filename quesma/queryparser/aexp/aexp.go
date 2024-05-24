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

func C(columnName string) *TableColumn {
	return &TableColumn{ColumnName: columnName}
}

func (e *TableColumn) Accept(v AExpVisitor) interface{} {
	return v.VisitTableColumn(e)
}

type Function struct {
	Name string
	Args []AExp
}

func (e *Function) Accept(v AExpVisitor) interface{} {
	return v.VisitFunction(e)
}

func FN(name string, args ...AExp) *Function {
	return &Function{Name: name, Args: args}
}

func Count(args ...AExp) *Function {
	return FN("COUNT", args...)
}

type Literal struct {
	Value any
}

func L(value any) *Literal {
	return &Literal{Value: value}
}

var Wildcard = &Literal{Value: "*"}

// it will render as IS
type symbol string

func Symbol(s string) *Literal {
	return L(symbol(s))
}

func (e *Literal) Accept(v AExpVisitor) interface{} {
	return v.VisitLiteral(e)
}

type Composite struct {
	Expressions []AExp
}

func (e *Composite) Accept(v AExpVisitor) interface{} {
	return v.VisitComposite(e)
}

func NewComposite(expressions ...AExp) *Composite {
	return &Composite{Expressions: expressions}
}

type SQL struct {
	Query string
}

func (s *SQL) Accept(v AExpVisitor) interface{} {
	return v.VisitSQL(s)
}

type AExpVisitor interface {
	VisitTableColumn(e *TableColumn) interface{}
	VisitFunction(e *Function) interface{}
	VisitLiteral(l *Literal) interface{}
	VisitComposite(e *Composite) interface{}
	VisitSQL(s *SQL) interface{}
}

type renderer struct{}

func (v *renderer) VisitTableColumn(e *TableColumn) interface{} {
	if e.TableName == "" {
		return e.ColumnName
	}
	return e.TableName + "." + e.ColumnName
}

func (v *renderer) VisitFunction(e *Function) interface{} {
	args := make([]string, 0)
	for _, arg := range e.Args {
		args = append(args, arg.Accept(v).(string))
	}
	return e.Name + "(" + strings.Join(args, ", ") + ")"

}

func (v *renderer) VisitLiteral(l *Literal) interface{} {
	return fmt.Sprintf("%s", l.Value)
}

func (v *renderer) VisitComposite(e *Composite) interface{} {
	exps := make([]string, 0)
	for _, exp := range e.Expressions {
		exps = append(exps, exp.Accept(v).(string))
	}
	return strings.Join(exps, " ")
}


func (v *renderer) VisitSQL(s *SQL) interface{} {
	return s.Query
}