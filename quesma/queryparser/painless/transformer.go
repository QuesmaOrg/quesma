// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painless

import (
	"fmt"
	painless_antlr "quesma/queryparser/painless/antlr"
)

type PainlessTransformer struct {
	painless_antlr.BasePainlessParserVisitor

	Errors []error
}

func NewPainlessTransformer() *PainlessTransformer {
	return &PainlessTransformer{}
}

func (v *PainlessTransformer) VisitStatement(ctx *painless_antlr.StatementContext) interface{} {
	fmt.Println("JM: VisitStatement")
	if ctx.Rstatement() != nil {
		v.Errors = append(v.Errors, fmt.Errorf("if/else/while/for/... not supported"))
		return nil
	}

	ctx.Dstatement().Accept(v)
	return nil
}

func (v *PainlessTransformer) VisitExpr(ctx *painless_antlr.ExprContext) interface{} {
	fmt.Println("JM: VisitExpr")

	return v.VisitChildren(ctx)
}

func (v *PainlessTransformer) VisitNonconditional(ctx *painless_antlr.NonconditionalContext) interface{} {
	fmt.Println("JM: VisitNonconditional")
	return v.VisitChildren(ctx)
}

func (v *PainlessTransformer) VisitCallinvoke(ctx *painless_antlr.CallinvokeContext) interface{} {
	fmt.Println("JM: VisitCallinvoke")
	return v.VisitChildren(ctx)
}
