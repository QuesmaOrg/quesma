// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package transform

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/eql/parser"
	"strconv"
	"strings"
)

type EQLParseTreeToExpTransformer struct {
	parser.BaseEQLVisitor

	// category field name can be customized
	// it's provided as a parameter in the query
	// default is "event.category"
	CategoryFieldName string

	Errors []string
}

func NewEQLParseTreeToExpTransformer() *EQLParseTreeToExpTransformer {
	return &EQLParseTreeToExpTransformer{
		CategoryFieldName: "event.category", // this is the default
	}
}

func (v *EQLParseTreeToExpTransformer) error(msg string) {
	v.Errors = append(v.Errors, msg)
}

func (v *EQLParseTreeToExpTransformer) evalString(s string) string {

	const tripleQuote = `"""`
	if strings.HasPrefix(s, tripleQuote) && strings.HasSuffix(s, tripleQuote) {
		return s[3 : len(s)-3]
	}

	const quote = `"`
	if strings.HasPrefix(s, quote) && strings.HasSuffix(s, quote) {
		// TODO handle escape sequences
		s = s[1 : len(s)-1]

		s = strings.ReplaceAll(s, `\"`, `"`)
		s = strings.ReplaceAll(s, `\\`, `\`)
		s = strings.ReplaceAll(s, `\n`, "\n")
		s = strings.ReplaceAll(s, `\t`, "\t")
		s = strings.ReplaceAll(s, `\r`, "\r")

	}

	return s
}

func (v *EQLParseTreeToExpTransformer) evalInteger(s string) (int, error) {
	return strconv.Atoi(s)
}

func (v *EQLParseTreeToExpTransformer) evalFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

func (v *EQLParseTreeToExpTransformer) VisitQuery(ctx *parser.QueryContext) interface{} {
	return ctx.SimpleQuery().Accept(v)
}

func (v *EQLParseTreeToExpTransformer) VisitSimpleQuery(ctx *parser.SimpleQueryContext) interface{} {

	category := ctx.Category().Accept(v)
	condition := ctx.Condition().Accept(v)

	if condition == nil {
		if category == nil {
			return nil // empty `where` clause
		} else {
			return category
		}
	} else {
		if category != nil {
			return NewInfixOp("and", condition.(Exp), category.(Exp))
		} else {
			return condition
		}
	}
}

func (v *EQLParseTreeToExpTransformer) VisitConditionBoolean(ctx *parser.ConditionBooleanContext) interface{} {
	return v.evalBoolean(ctx.GetText())
}

func (v *EQLParseTreeToExpTransformer) VisitConditionLogicalOp(ctx *parser.ConditionLogicalOpContext) interface{} {
	left := ctx.GetLeft().Accept(v)
	right := ctx.GetRight().Accept(v)
	op := strings.ToLower(ctx.GetOp().GetText())

	return NewInfixOp(op, left.(Exp), right.(Exp))
}

func (v *EQLParseTreeToExpTransformer) VisitComparisonOp(ctx *parser.ComparisonOpContext) interface{} {

	op := ctx.GetOp().GetText()
	left := ctx.GetLeft().Accept(v)
	right := ctx.GetRight().Accept(v)

	return NewInfixOp(op, left.(Exp), right.(Exp))
}

func (v *EQLParseTreeToExpTransformer) VisitLookupOpList(ctx *parser.LookupOpListContext) interface{} {

	field := ctx.Field().Accept(v)
	list := ctx.GetList().Accept(v)
	op := ctx.GetOp().GetText()

	op = strings.ToLower(op)
	// paranoia check, should never happen
	// if there is no visitor implemented for the right side value is null

	// TODO add more info here to help debugging
	if list == nil {
		v.error("value is nil here")
		return &Const{Value: "error"}
	}

	return NewInfixOp(op, field.(Exp), list.(Exp))
}

func (v *EQLParseTreeToExpTransformer) VisitLookupNotOpList(ctx *parser.LookupNotOpListContext) interface{} {

	field := ctx.Field().Accept(v)
	list := ctx.GetList().Accept(v)
	op := ctx.GetOp().GetText()
	op = strings.ToLower(op)

	return NewInfixOp("not "+op, field.(Exp), list.(Exp))
}

func (v *EQLParseTreeToExpTransformer) VisitConditionNot(ctx *parser.ConditionNotContext) interface{} {
	inner := ctx.Condition().Accept(v)
	return NewPrefixOp("not", []Exp{inner.(Exp)})
}

func (v *EQLParseTreeToExpTransformer) VisitConditionGroup(ctx *parser.ConditionGroupContext) interface{} {
	return NewGroup(ctx.Condition().Accept(v).(Exp))
}

func (v *EQLParseTreeToExpTransformer) VisitConditionFuncall(ctx *parser.ConditionFuncallContext) interface{} {

	return ctx.Funcall().Accept(v)
}

func (v *EQLParseTreeToExpTransformer) VisitField(ctx *parser.FieldContext) interface{} {

	name := v.evalString(ctx.GetText())

	if strings.HasPrefix(name, `?`) {
		v.error("optional fields are not supported")
	}

	return NewSymbol(name)

}

func (v *EQLParseTreeToExpTransformer) VisitValueLiteral(ctx *parser.ValueLiteralContext) interface{} {

	return ctx.Literal().Accept(v)
}

func (v *EQLParseTreeToExpTransformer) VisitLiteral(ctx *parser.LiteralContext) interface{} {
	switch {

	case ctx.STRING() != nil:
		return &Const{Value: v.evalString(ctx.GetText())}
	case ctx.NUMBER() != nil:

		i, err := v.evalInteger(ctx.GetText())
		if err == nil {
			return &Const{Value: i}
		}

		f, err := v.evalFloat(ctx.GetText())
		if err == nil {
			return &Const{Value: f}
		}

		v.error(fmt.Sprintf("error parsing number: %v", err))
		return &Const{Value: 0}

	case ctx.BOOLEAN() != nil:
		return v.evalBoolean(ctx.GetText())
	}

	return nil
}

func (v *EQLParseTreeToExpTransformer) VisitValueGroup(ctx *parser.ValueGroupContext) interface{} {

	return NewGroup(ctx.Value().Accept(v).(Exp))

}

func (v *EQLParseTreeToExpTransformer) VisitValueFuncall(ctx *parser.ValueFuncallContext) interface{} {
	return ctx.Funcall().Accept(v)
}

func (v *EQLParseTreeToExpTransformer) VisitValueAddSub(ctx *parser.ValueAddSubContext) interface{} {
	left := ctx.GetLeft().Accept(v)
	right := ctx.GetRight().Accept(v)
	op := ctx.GetOp().GetText()
	return NewInfixOp(op, left.(Exp), right.(Exp))
}

func (v *EQLParseTreeToExpTransformer) VisitValueMulDiv(ctx *parser.ValueMulDivContext) interface{} {
	left := ctx.GetLeft().Accept(v)
	right := ctx.GetRight().Accept(v)
	op := ctx.GetOp().GetText()
	return NewInfixOp(op, left.(Exp), right.(Exp))
}

func (v *EQLParseTreeToExpTransformer) VisitValueField(ctx *parser.ValueFieldContext) interface{} {
	return ctx.Field().Accept(v)
}

func (v *EQLParseTreeToExpTransformer) VisitValueNull(ctx *parser.ValueNullContext) interface{} {
	return NULL
}

func (v *EQLParseTreeToExpTransformer) VisitFuncall(ctx *parser.FuncallContext) interface{} {

	name := ctx.FuncName().Accept(v).(string)

	var args []Exp

	for _, a := range ctx.AllValue() {
		args = append(args, a.Accept(v).(Exp))
	}

	return NewFunction(name, args...)

}

func (v *EQLParseTreeToExpTransformer) VisitFuncName(ctx *parser.FuncNameContext) interface{} {
	return ctx.GetText()
}

func (v *EQLParseTreeToExpTransformer) VisitLiteralList(ctx *parser.LiteralListContext) interface{} {

	var values []Exp

	for _, l := range ctx.AllLiteral() {
		values = append(values, l.Accept(v).(Exp))
	}

	return NewArray(values...)
}

func (v *EQLParseTreeToExpTransformer) VisitCategory(ctx *parser.CategoryContext) interface{} {

	var category string
	switch {
	case ctx.ID() != nil:
		category = ctx.ID().GetText()
	case ctx.STRING() != nil:
		category = v.evalString(ctx.STRING().GetText())
	case ctx.ANY() != nil:
	default:
	}

	if category != "" {
		return NewInfixOp("==", NewSymbol(v.CategoryFieldName), NewConst(category))
	}
	// match all
	return nil
}

func (v *EQLParseTreeToExpTransformer) evalBoolean(s string) Exp {
	if strings.ToLower(s) == "true" {
		return TRUE
	}

	return FALSE
}
