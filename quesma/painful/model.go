// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painful

//go:generate pigeon -support-left-recursion -o generated_parser.go painless.peg

import (
	"fmt"
)

type Env struct {
	Doc map[string]any

	EmitFieldName string
}

type Expr interface {
	Eval(env *Env) (any, error)
}

type Literal struct {
	Value any
}

func (l *Literal) Eval(env *Env) (any, error) {
	return l.Value, nil
}

type InfixOpExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

func (i *InfixOpExpr) Eval(env *Env) (any, error) {

	left, err := i.Left.Eval(env)
	if err != nil {
		return nil, err
	}

	right, err := i.Right.Eval(env)
	if err != nil {
		return nil, err
	}

	switch i.Op {

	case "+":

		switch left.(type) {

		case string:
			return fmt.Sprintf("%v%v", left, right), nil

		default:
			return fmt.Sprintf("%v%v", left, right), nil
		}

	default:

		return nil, fmt.Errorf("unknown operator: %s", i.Op)

	}
}

type ConditionalExpr struct {
	Cond Expr
	Then Expr
	Else Expr
}

func (c *ConditionalExpr) Eval(env *Env) (any, error) {

	cond, err := c.Cond.Eval(env)
	if err != nil {
		return nil, err
	}

	if cond.(bool) {
		return c.Then.Eval(env)
	}

	return c.Else.Eval(env)
}

type DocExpr struct {
	FieldName string
}

func (d *DocExpr) Eval(env *Env) (any, error) {
	return env.Doc[d.FieldName], nil
}

type EmitExpr struct {
	Expr Expr
}

func (e *EmitExpr) Eval(env *Env) (any, error) {

	val, err := e.Expr.Eval(env)
	if err != nil {
		return nil, err
	}

	env.Doc[env.EmitFieldName] = val

	return val, nil
}

type AccessorExpr struct {
	Expr         Expr
	PropertyName string
}

func (a *AccessorExpr) Eval(env *Env) (any, error) {

	val, err := a.Expr.Eval(env)
	if err != nil {
		return nil, err
	}

	// value property is a special case
	// it's just a current value of the expression
	if a.PropertyName == "value" {
		return val, nil
	}

	switch v := val.(type) {

	case map[string]any:

		return v[a.PropertyName], nil
	default:
		return nil, fmt.Errorf("cannot access field %s on %T", a.PropertyName, val)
	}
}

type MethodCallExpr struct {
	Expr       Expr
	MethodName string
	Args       []Expr
}

func (m *MethodCallExpr) Eval(env *Env) (any, error) {

	val, err := m.Expr.Eval(env)
	if err != nil {
		return nil, err
	}

	switch m.MethodName {

	case "getHour":
		// TODO add parse HOUR here
		return val, nil
	default:
		return nil, fmt.Errorf("unknown method %s", m.MethodName)
	}
}

func ExpectExpr(potentialExpr any) (Expr, error) {

	switch expr := potentialExpr.(type) {
	case Expr:
		return expr, nil
	default:
		return nil, fmt.Errorf("expected expression, got %T", potentialExpr)
	}
}

func ExpectString(potentialExpr any) (string, error) {

	switch str := potentialExpr.(type) {
	case string:
		return str, nil
	default:
		return "", fmt.Errorf("expected string, got %T", potentialExpr)
	}
}
