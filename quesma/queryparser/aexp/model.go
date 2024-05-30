package aexp

import (
	"fmt"
	"strings"
)

type AExp interface {
	Accept(v AExpVisitor) interface{}
	String() string
}

func RenderSQL(exp AExp) string {
	return exp.Accept(&renderer{}).(string)
}

type TableColumnExp struct {
	TableAlias string
	ColumnName string
}

func (e TableColumnExp) String() string {
	return fmt.Sprintf("(tablecolumn '%s' . '%s') ", e.TableAlias, e.ColumnName)
}

func (e TableColumnExp) Accept(v AExpVisitor) interface{} {
	return v.VisitTableColumn(e)
}

type FunctionExp struct {
	Name string
	Args []AExp
}

func (e FunctionExp) String() string {
	var args []string
	for _, arg := range e.Args {
		args = append(args, arg.String())
	}
	return fmt.Sprintf("(function %s %s)", e.Name, strings.Join(args, " "))
}

func (e FunctionExp) Accept(v AExpVisitor) interface{} {
	return v.VisitFunction(e)
}

// It represents functions with multitple arguments list
// like `quantile(level)(expr)
type MultiFunctionExp struct {
	Name string
	Args []AExp
}

func (e MultiFunctionExp) String() string {
	var args []string
	for _, arg := range e.Args {
		args = append(args, arg.String())
	}
	return fmt.Sprintf("(multifunction %s %s)", e.Name, strings.Join(args, " "))
}

func (e MultiFunctionExp) Accept(v AExpVisitor) interface{} {
	return v.VisitMultiFunction(e)
}

type LiteralExp struct {
	Value any
}

func (e LiteralExp) String() string {
	return fmt.Sprintf("(literal %s)", e.Value)
}

func (e LiteralExp) Accept(v AExpVisitor) interface{} {
	return v.VisitLiteral(e)
}

// Space separated expressions
type CompositeExp struct {
	Expressions []AExp
}

func (e CompositeExp) Accept(v AExpVisitor) interface{} {
	return v.VisitComposite(e)
}

func (e CompositeExp) String() string {

	var exps []string
	for _, exp := range e.Expressions {
		exps = append(exps, exp.String())
	}

	return fmt.Sprintf("(composite %s)", strings.Join(exps, " "))
}

type InfixExp struct {
	Left  AExp
	Op    string
	Right AExp
}

func (e InfixExp) String() string {
	return fmt.Sprintf("(infix %s %s %s)", e.Left, e.Op, e.Right)
}

func (e InfixExp) Accept(v AExpVisitor) interface{} {
	return v.VisitInfix(e)
}

// ASIS expressions, this is workaroung for not supported expressions
// It can be named as TODO.
type SQL struct {
	Query string
}

func (s SQL) String() string {
	return fmt.Sprintf("(sql '%s')", s.Query)
}

func (s SQL) Accept(v AExpVisitor) interface{} {
	return v.VisitSQL(s)
}

type AExpVisitor interface {
	VisitTableColumn(e TableColumnExp) interface{}
	VisitFunction(e FunctionExp) interface{}
	VisitMultiFunction(e MultiFunctionExp) interface{}
	VisitLiteral(l LiteralExp) interface{}
	VisitComposite(e CompositeExp) interface{}
	VisitInfix(e InfixExp) interface{}
	VisitSQL(s SQL) interface{}
}
