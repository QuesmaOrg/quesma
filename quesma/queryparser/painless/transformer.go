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

func (v *PainlessTransformer) VisitFunction(ctx *painless_antlr.FunctionContext) interface{} {
	fmt.Println("JM: VisitFunction")
	return ctx.Accept(v)
}

func (v *PainlessTransformer) VisitSource(ctx *painless_antlr.SourceContext) interface{} {

	fmt.Println("JM: VisitSource")

	for _, s := range ctx.AllFunction() {
		s.Accept(v)
	}

	for _, s := range ctx.AllStatement() {
		return s.Accept(v)
	}

	return nil
}

func (v *PainlessTransformer) VisitStatement(ctx *painless_antlr.StatementContext) interface{} {
	fmt.Println("JM: VisitStatement")
	if ctx.Rstatement() != nil {
		v.Errors = append(v.Errors, fmt.Errorf("if/else/while/for/... not supported"))
		return nil
	}

	return ctx.Dstatement().Accept(v)
}

func (v *PainlessTransformer) VisitCalllocal(ctx *painless_antlr.CalllocalContext) interface{} {
	fmt.Println("JM: VisitCalllocal", ctx.ID())

	fn := ctx.ID().GetText()

	if fn != "emit" {
		v.Errors = append(v.Errors, fmt.Errorf("only emit function is supported"))
		return nil
	}

	args := ctx.Arguments().Accept(v)

	fmt.Println("ARGS : ", args)
	return fmt.Sprintf("%s(%s)", ctx.ID(), args)
}

func (v *PainlessTransformer) VisitExpr(ctx *painless_antlr.ExprContext) interface{} {
	fmt.Println("JM: VisitExpr")

	return ctx.Expression().Accept(v)
}

func (v *PainlessTransformer) VisitNonconditional(ctx *painless_antlr.NonconditionalContext) interface{} {
	fmt.Println("JM: VisitNonconditional")
	return ctx.Noncondexpression().Accept(v)
}

func (v *PainlessTransformer) VisitCallinvoke(ctx *painless_antlr.CallinvokeContext) interface{} {
	fmt.Println("JM: VisitCallinvoke")

	return fmt.Sprintf("%s(%s)", ctx.DOTID(), ctx.Arguments().Accept(v))
}

func (v *PainlessTransformer) VisitArguments(ctx *painless_antlr.ArgumentsContext) interface{} {
	fmt.Println("JM: VisitArguments", ctx.GetText(), len(ctx.AllArgument()))

	args := []any{}

	for n, a := range ctx.AllArgument() {
		fmt.Println("JM: VisitArguments: args", n, a.GetText())
		args = append(args, a.Accept(v))
	}

	fmt.Println("JM: VisitArguments: args", args)
	return args
}

func (v *PainlessTransformer) VisitArgument(ctx *painless_antlr.ArgumentContext) interface{} {
	fmt.Println("JM: VisitArgument", ctx.GetText())

	if ctx.Expression() != nil {
		return ctx.Expression().Accept(v)
	}

	if ctx.Lambda() != nil {
		return ctx.Lambda().Accept(v)
	}

	if ctx.Funcref() != nil {
		return ctx.Funcref().Accept(v)
	}
	return nil
}

func (v *PainlessTransformer) VisitSingle(ctx *painless_antlr.SingleContext) interface{} {
	fmt.Println("JM: VisitSingle")
	return ctx.Unary().Accept(v)

}

func (v *PainlessTransformer) VisitNotaddsub(ctx *painless_antlr.NotaddsubContext) interface{} {
	fmt.Println("JM: VisitNotaddsub")
	return ctx.Unarynotaddsub().Accept(v)
}

func (v *PainlessTransformer) VisitRead(ctx *painless_antlr.ReadContext) interface{} {
	fmt.Println("JM: VisitRead")
	return ctx.Chain().Accept(v)
}

func (v *PainlessTransformer) VisitDynamic(ctx *painless_antlr.DynamicContext) interface{} {
	fmt.Println("JM: VisitDynamic", ctx.GetText(), ctx.Primary().GetText())

	name := ctx.Primary().Accept(v)

	args := []any{}

	for _, postfix := range ctx.AllPostfix() {

		a := postfix.Accept(v)
		args = append(args, a)
	}

	fmt.Println("args", args)

	return fmt.Sprintf("(%s %s)", name, args)
}

func (v *PainlessTransformer) VisitPostfix(ctx *painless_antlr.PostfixContext) interface{} {
	fmt.Println("JM: VisitPostFix", ctx.GetText())

	if ctx.Fieldaccess() != nil {
		return ctx.Fieldaccess().Accept(v)
	}

	if ctx.Braceaccess() != nil {
		return ctx.Braceaccess().Accept(v)
	}

	if ctx.Callinvoke() != nil {
		return ctx.Callinvoke().Accept(v)
	}

	return nil
}

func (v *PainlessTransformer) VisitFieldaccess(ctx *painless_antlr.FieldaccessContext) interface{} {
	fmt.Println("JM: VisitFieldAccess")

	return ctx.DOTID().GetText()

}

func (v *PainlessTransformer) VisitVariable(ctx *painless_antlr.VariableContext) interface{} {
	fmt.Println("JM: VisitVariable", ctx.GetText())
	return ctx.ID().GetText()
}

func (v *PainlessTransformer) VisitBraceaccess(ctx *painless_antlr.BraceaccessContext) interface{} {
	fmt.Println("JM: VisitBraceAccess")

	return ctx.Expression().Accept(v)
}

func (v *PainlessTransformer) VisitString(ctx *painless_antlr.StringContext) interface{} {
	fmt.Println("JM: VisitString")

	return ctx.STRING()
}
