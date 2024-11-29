// Code generated from quesma/queryparser/painless/antlr/PainlessParser.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // PainlessParser
import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by PainlessParser.
type PainlessParserVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by PainlessParser#source.
	VisitSource(ctx *SourceContext) interface{}

	// Visit a parse tree produced by PainlessParser#function.
	VisitFunction(ctx *FunctionContext) interface{}

	// Visit a parse tree produced by PainlessParser#parameters.
	VisitParameters(ctx *ParametersContext) interface{}

	// Visit a parse tree produced by PainlessParser#statement.
	VisitStatement(ctx *StatementContext) interface{}

	// Visit a parse tree produced by PainlessParser#if.
	VisitIf(ctx *IfContext) interface{}

	// Visit a parse tree produced by PainlessParser#while.
	VisitWhile(ctx *WhileContext) interface{}

	// Visit a parse tree produced by PainlessParser#for.
	VisitFor(ctx *ForContext) interface{}

	// Visit a parse tree produced by PainlessParser#each.
	VisitEach(ctx *EachContext) interface{}

	// Visit a parse tree produced by PainlessParser#ineach.
	VisitIneach(ctx *IneachContext) interface{}

	// Visit a parse tree produced by PainlessParser#try.
	VisitTry(ctx *TryContext) interface{}

	// Visit a parse tree produced by PainlessParser#do.
	VisitDo(ctx *DoContext) interface{}

	// Visit a parse tree produced by PainlessParser#decl.
	VisitDecl(ctx *DeclContext) interface{}

	// Visit a parse tree produced by PainlessParser#continue.
	VisitContinue(ctx *ContinueContext) interface{}

	// Visit a parse tree produced by PainlessParser#break.
	VisitBreak(ctx *BreakContext) interface{}

	// Visit a parse tree produced by PainlessParser#return.
	VisitReturn(ctx *ReturnContext) interface{}

	// Visit a parse tree produced by PainlessParser#throw.
	VisitThrow(ctx *ThrowContext) interface{}

	// Visit a parse tree produced by PainlessParser#expr.
	VisitExpr(ctx *ExprContext) interface{}

	// Visit a parse tree produced by PainlessParser#trailer.
	VisitTrailer(ctx *TrailerContext) interface{}

	// Visit a parse tree produced by PainlessParser#block.
	VisitBlock(ctx *BlockContext) interface{}

	// Visit a parse tree produced by PainlessParser#empty.
	VisitEmpty(ctx *EmptyContext) interface{}

	// Visit a parse tree produced by PainlessParser#initializer.
	VisitInitializer(ctx *InitializerContext) interface{}

	// Visit a parse tree produced by PainlessParser#afterthought.
	VisitAfterthought(ctx *AfterthoughtContext) interface{}

	// Visit a parse tree produced by PainlessParser#declaration.
	VisitDeclaration(ctx *DeclarationContext) interface{}

	// Visit a parse tree produced by PainlessParser#decltype.
	VisitDecltype(ctx *DecltypeContext) interface{}

	// Visit a parse tree produced by PainlessParser#type.
	VisitType(ctx *TypeContext) interface{}

	// Visit a parse tree produced by PainlessParser#declvar.
	VisitDeclvar(ctx *DeclvarContext) interface{}

	// Visit a parse tree produced by PainlessParser#trap.
	VisitTrap(ctx *TrapContext) interface{}

	// Visit a parse tree produced by PainlessParser#single.
	VisitSingle(ctx *SingleContext) interface{}

	// Visit a parse tree produced by PainlessParser#comp.
	VisitComp(ctx *CompContext) interface{}

	// Visit a parse tree produced by PainlessParser#bool.
	VisitBool(ctx *BoolContext) interface{}

	// Visit a parse tree produced by PainlessParser#binary.
	VisitBinary(ctx *BinaryContext) interface{}

	// Visit a parse tree produced by PainlessParser#elvis.
	VisitElvis(ctx *ElvisContext) interface{}

	// Visit a parse tree produced by PainlessParser#instanceof.
	VisitInstanceof(ctx *InstanceofContext) interface{}

	// Visit a parse tree produced by PainlessParser#nonconditional.
	VisitNonconditional(ctx *NonconditionalContext) interface{}

	// Visit a parse tree produced by PainlessParser#conditional.
	VisitConditional(ctx *ConditionalContext) interface{}

	// Visit a parse tree produced by PainlessParser#assignment.
	VisitAssignment(ctx *AssignmentContext) interface{}

	// Visit a parse tree produced by PainlessParser#pre.
	VisitPre(ctx *PreContext) interface{}

	// Visit a parse tree produced by PainlessParser#addsub.
	VisitAddsub(ctx *AddsubContext) interface{}

	// Visit a parse tree produced by PainlessParser#notaddsub.
	VisitNotaddsub(ctx *NotaddsubContext) interface{}

	// Visit a parse tree produced by PainlessParser#read.
	VisitRead(ctx *ReadContext) interface{}

	// Visit a parse tree produced by PainlessParser#post.
	VisitPost(ctx *PostContext) interface{}

	// Visit a parse tree produced by PainlessParser#not.
	VisitNot(ctx *NotContext) interface{}

	// Visit a parse tree produced by PainlessParser#cast.
	VisitCast(ctx *CastContext) interface{}

	// Visit a parse tree produced by PainlessParser#primordefcast.
	VisitPrimordefcast(ctx *PrimordefcastContext) interface{}

	// Visit a parse tree produced by PainlessParser#refcast.
	VisitRefcast(ctx *RefcastContext) interface{}

	// Visit a parse tree produced by PainlessParser#primordefcasttype.
	VisitPrimordefcasttype(ctx *PrimordefcasttypeContext) interface{}

	// Visit a parse tree produced by PainlessParser#refcasttype.
	VisitRefcasttype(ctx *RefcasttypeContext) interface{}

	// Visit a parse tree produced by PainlessParser#dynamic.
	VisitDynamic(ctx *DynamicContext) interface{}

	// Visit a parse tree produced by PainlessParser#newarray.
	VisitNewarray(ctx *NewarrayContext) interface{}

	// Visit a parse tree produced by PainlessParser#precedence.
	VisitPrecedence(ctx *PrecedenceContext) interface{}

	// Visit a parse tree produced by PainlessParser#numeric.
	VisitNumeric(ctx *NumericContext) interface{}

	// Visit a parse tree produced by PainlessParser#true.
	VisitTrue(ctx *TrueContext) interface{}

	// Visit a parse tree produced by PainlessParser#false.
	VisitFalse(ctx *FalseContext) interface{}

	// Visit a parse tree produced by PainlessParser#null.
	VisitNull(ctx *NullContext) interface{}

	// Visit a parse tree produced by PainlessParser#string.
	VisitString(ctx *StringContext) interface{}

	// Visit a parse tree produced by PainlessParser#regex.
	VisitRegex(ctx *RegexContext) interface{}

	// Visit a parse tree produced by PainlessParser#listinit.
	VisitListinit(ctx *ListinitContext) interface{}

	// Visit a parse tree produced by PainlessParser#mapinit.
	VisitMapinit(ctx *MapinitContext) interface{}

	// Visit a parse tree produced by PainlessParser#variable.
	VisitVariable(ctx *VariableContext) interface{}

	// Visit a parse tree produced by PainlessParser#calllocal.
	VisitCalllocal(ctx *CalllocalContext) interface{}

	// Visit a parse tree produced by PainlessParser#newobject.
	VisitNewobject(ctx *NewobjectContext) interface{}

	// Visit a parse tree produced by PainlessParser#postfix.
	VisitPostfix(ctx *PostfixContext) interface{}

	// Visit a parse tree produced by PainlessParser#postdot.
	VisitPostdot(ctx *PostdotContext) interface{}

	// Visit a parse tree produced by PainlessParser#callinvoke.
	VisitCallinvoke(ctx *CallinvokeContext) interface{}

	// Visit a parse tree produced by PainlessParser#fieldaccess.
	VisitFieldaccess(ctx *FieldaccessContext) interface{}

	// Visit a parse tree produced by PainlessParser#braceaccess.
	VisitBraceaccess(ctx *BraceaccessContext) interface{}

	// Visit a parse tree produced by PainlessParser#newstandardarray.
	VisitNewstandardarray(ctx *NewstandardarrayContext) interface{}

	// Visit a parse tree produced by PainlessParser#newinitializedarray.
	VisitNewinitializedarray(ctx *NewinitializedarrayContext) interface{}

	// Visit a parse tree produced by PainlessParser#listinitializer.
	VisitListinitializer(ctx *ListinitializerContext) interface{}

	// Visit a parse tree produced by PainlessParser#mapinitializer.
	VisitMapinitializer(ctx *MapinitializerContext) interface{}

	// Visit a parse tree produced by PainlessParser#maptoken.
	VisitMaptoken(ctx *MaptokenContext) interface{}

	// Visit a parse tree produced by PainlessParser#arguments.
	VisitArguments(ctx *ArgumentsContext) interface{}

	// Visit a parse tree produced by PainlessParser#argument.
	VisitArgument(ctx *ArgumentContext) interface{}

	// Visit a parse tree produced by PainlessParser#lambda.
	VisitLambda(ctx *LambdaContext) interface{}

	// Visit a parse tree produced by PainlessParser#lamtype.
	VisitLamtype(ctx *LamtypeContext) interface{}

	// Visit a parse tree produced by PainlessParser#classfuncref.
	VisitClassfuncref(ctx *ClassfuncrefContext) interface{}

	// Visit a parse tree produced by PainlessParser#constructorfuncref.
	VisitConstructorfuncref(ctx *ConstructorfuncrefContext) interface{}

	// Visit a parse tree produced by PainlessParser#localfuncref.
	VisitLocalfuncref(ctx *LocalfuncrefContext) interface{}
}
