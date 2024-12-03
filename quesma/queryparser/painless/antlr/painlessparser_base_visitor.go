// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
// Code generated from quesma/queryparser/painless/antlr/PainlessParser.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // PainlessParser
import "github.com/antlr4-go/antlr/v4"

type BasePainlessParserVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BasePainlessParserVisitor) VisitSource(ctx *SourceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitFunction(ctx *FunctionContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitParameters(ctx *ParametersContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitStatement(ctx *StatementContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitIf(ctx *IfContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitWhile(ctx *WhileContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitFor(ctx *ForContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitEach(ctx *EachContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitIneach(ctx *IneachContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitTry(ctx *TryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitDo(ctx *DoContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitDecl(ctx *DeclContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitContinue(ctx *ContinueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitBreak(ctx *BreakContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitReturn(ctx *ReturnContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitThrow(ctx *ThrowContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitExpr(ctx *ExprContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitTrailer(ctx *TrailerContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitBlock(ctx *BlockContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitEmpty(ctx *EmptyContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitInitializer(ctx *InitializerContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitAfterthought(ctx *AfterthoughtContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitDeclaration(ctx *DeclarationContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitDecltype(ctx *DecltypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitType(ctx *TypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitDeclvar(ctx *DeclvarContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitTrap(ctx *TrapContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitSingle(ctx *SingleContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitComp(ctx *CompContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitBool(ctx *BoolContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitBinary(ctx *BinaryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitElvis(ctx *ElvisContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitInstanceof(ctx *InstanceofContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitNonconditional(ctx *NonconditionalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitConditional(ctx *ConditionalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitAssignment(ctx *AssignmentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitPre(ctx *PreContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitAddsub(ctx *AddsubContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitNotaddsub(ctx *NotaddsubContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitRead(ctx *ReadContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitPost(ctx *PostContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitNot(ctx *NotContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitCast(ctx *CastContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitPrimordefcast(ctx *PrimordefcastContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitRefcast(ctx *RefcastContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitPrimordefcasttype(ctx *PrimordefcasttypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitRefcasttype(ctx *RefcasttypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitDynamic(ctx *DynamicContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitNewarray(ctx *NewarrayContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitPrecedence(ctx *PrecedenceContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitNumeric(ctx *NumericContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitTrue(ctx *TrueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitFalse(ctx *FalseContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitNull(ctx *NullContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitString(ctx *StringContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitRegex(ctx *RegexContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitListinit(ctx *ListinitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitMapinit(ctx *MapinitContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitVariable(ctx *VariableContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitCalllocal(ctx *CalllocalContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitNewobject(ctx *NewobjectContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitPostfix(ctx *PostfixContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitPostdot(ctx *PostdotContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitCallinvoke(ctx *CallinvokeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitFieldaccess(ctx *FieldaccessContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitBraceaccess(ctx *BraceaccessContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitNewstandardarray(ctx *NewstandardarrayContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitNewinitializedarray(ctx *NewinitializedarrayContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitListinitializer(ctx *ListinitializerContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitMapinitializer(ctx *MapinitializerContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitMaptoken(ctx *MaptokenContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitArguments(ctx *ArgumentsContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitArgument(ctx *ArgumentContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitLambda(ctx *LambdaContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitLamtype(ctx *LamtypeContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitClassfuncref(ctx *ClassfuncrefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitConstructorfuncref(ctx *ConstructorfuncrefContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BasePainlessParserVisitor) VisitLocalfuncref(ctx *LocalfuncrefContext) interface{} {
	return v.VisitChildren(ctx)
}
