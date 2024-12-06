// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painful

//go:generate pigeon -nolint -support-left-recursion -o generated_parser.go painless.peg

import (
	"fmt"
	"time"
)

func ParsePainless(script string) (Expr, error) {

	evalTree, err := Parse("", []byte(script))
	if err != nil {
		return nil, err
	}

	switch expr := evalTree.(type) {
	case Expr:
		return expr, nil

	default:
		return nil, fmt.Errorf("not an painless expression")
	}

}

type Env struct {
	Doc map[string]any

	EmitValue any
}

type Expr interface {
	Eval(env *Env) (any, error)
}

type LiteralExpr struct {
	Value any
}

func (l *LiteralExpr) Eval(env *Env) (any, error) {
	return l.Value, nil
}

type InfixOpExpr struct {
	Position string
	Left     Expr
	Op       string
	Right    Expr
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

		return nil, fmt.Errorf("%s: '%s' operator is not supported", i.Position, i.Op)

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
	FieldName Expr
}

func (d *DocExpr) Eval(env *Env) (any, error) {

	fieldName, err := d.FieldName.Eval(env)
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("%v", fieldName)
	return env.Doc[key], nil
}

type EmitExpr struct {
	Expr Expr
}

func (e *EmitExpr) Eval(env *Env) (any, error) {

	val, err := e.Expr.Eval(env)
	if err != nil {
		return nil, err
	}

	env.EmitValue = val

	return val, nil
}

type AccessorExpr struct {
	Position     string
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

	// for testing purposes
	if a.PropertyName == "type" {
		return fmt.Sprintf("%T", val), nil
	}

	return nil, fmt.Errorf("%s: '%s' property is not supported", a.Position, a.PropertyName)

}

type MethodCallExpr struct {
	Position   string
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

		typeVal, err := ExpectDate(val)

		if err != nil {
			return nil, fmt.Errorf("%s: method '%s' failed to coerce '%v' into a datetime: %v ", m.Position, m.MethodName, val, err)
		}

		return typeVal.Hour(), nil

	case "formatISO8601": // TODO maybe more easier to remember name

		typeVal, err := ExpectDate(val)

		if err != nil {
			return nil, fmt.Errorf("%s: method '%s' failed to coerce '%v' into a datetime: %v ", m.Position, m.MethodName, val, err)
		}

		return typeVal.Format(time.RFC3339), nil

	default:
		return nil, fmt.Errorf("%s: '%s' method is not supported", m.Position, m.MethodName)
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

func ExpectDate(potentialExpr any) (time.Time, error) {

	switch date := potentialExpr.(type) {
	case time.Time:
		return date, nil

	case string:

		formats := []string{
			"Jan 2, 2006 @ 15:04:05.000 -0700 MST", // this format in example provided by Kibana\
			"2006-01-02 15:04:05.000 -0700 MST",    // clickhouse format
			time.Layout,
			time.ANSIC,
			time.UnixDate,
			time.RubyDate,
			time.RFC822,
			time.RFC822Z,
			time.RFC850,
			time.RFC1123,
			time.RFC1123Z,
			time.RFC3339,
			time.RFC3339Nano,
			time.RFC3339,
		}

		for _, format := range formats {
			t, err := time.Parse(format, date)
			if err == nil {
				return t, nil
			}
		}

		return time.Time{}, fmt.Errorf("failed to parse date: %s", date)
	default:
		return time.Time{}, fmt.Errorf("expected date, got %T", potentialExpr)
	}
}
