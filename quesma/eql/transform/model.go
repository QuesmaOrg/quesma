// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package transform

import "fmt"

type Exp interface {
	Accept(v ExpVisitor) interface{}

	// TODO add debug or context information here
	// we should know the source of the expression
}

type Const struct {
	Value interface{}
}

func NewConst(value interface{}) *Const {
	return &Const{Value: value}
}

var TRUE = NewConst(true)
var FALSE = NewConst(false)

func (e *Const) String() string {
	return fmt.Sprintf("(const '%v')", e.Value)
}

func (e *Const) Accept(v ExpVisitor) interface{} {
	return v.VisitConst(e)
}

type Symbol struct {
	Name string
}

func NewSymbol(name string) *Symbol {
	return &Symbol{Name: name}
}

func (e *Symbol) String() string {
	return fmt.Sprintf("(symbol %v)", e.Name)
}

func (e *Symbol) Accept(v ExpVisitor) interface{} {
	return v.VisitSymbol(e)
}

var NULL = &Symbol{Name: "NULL"}

type Group struct {
	Inner Exp
}

func NewGroup(inner Exp) *Group {
	return &Group{Inner: inner}
}

func (e *Group) String() string {
	return fmt.Sprintf("(group %v)", e.Inner)
}

func (e *Group) Accept(v ExpVisitor) interface{} {
	return v.VisitGroup(e)
}

type InfixOp struct {
	Op    string
	Left  Exp
	Right Exp
}

func NewInfixOp(op string, left, right Exp) *InfixOp {
	return &InfixOp{
		Op:    op,
		Left:  left,
		Right: right,
	}
}

func (e *InfixOp) String() string {
	return fmt.Sprintf("(infix '%v' %v %v)", e.Op, e.Left, e.Right)
}

func (e *InfixOp) Accept(v ExpVisitor) interface{} {
	return v.VisitInfixOp(e)
}

type PrefixOp struct {
	Op   string
	Args []Exp
}

func NewPrefixOp(op string, args []Exp) *PrefixOp {
	return &PrefixOp{
		Op:   op,
		Args: args,
	}
}

func (e *PrefixOp) String() string {
	return fmt.Sprintf("(prefix '%v' %v)", e.Op, e.Args)
}

func (e *PrefixOp) Accept(v ExpVisitor) interface{} {
	return v.VisitPrefixOp(e)
}

type Function struct {
	Name Symbol
	Args []Exp
}

func NewFunction(name string, args ...Exp) *Function {
	return &Function{
		Name: Symbol{Name: name},
		Args: args,
	}
}

func (e *Function) String() string {
	return fmt.Sprintf("(function %v %v)", e.Name, e.Args)
}

func (e *Function) Accept(v ExpVisitor) interface{} {
	return v.VisitFunction(e)
}

type Array struct {
	Values []Exp
}

func NewArray(values ...Exp) *Array {
	return &Array{Values: values}
}

func (e *Array) String() string {
	return fmt.Sprintf("(array %v)", e.Values)
}

func (e *Array) Accept(v ExpVisitor) interface{} {
	return v.VisitArray(e)
}

type ExpVisitor interface {
	VisitConst(e *Const) interface{}
	VisitSymbol(e *Symbol) interface{}
	VisitGroup(e *Group) interface{}
	VisitInfixOp(e *InfixOp) interface{}
	VisitPrefixOp(e *PrefixOp) interface{}
	VisitFunction(e *Function) interface{}
	VisitArray(e *Array) interface{}
}
