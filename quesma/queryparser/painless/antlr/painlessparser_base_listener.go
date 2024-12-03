// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
// Code generated from quesma/queryparser/painless/antlr/PainlessParser.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // PainlessParser
import "github.com/antlr4-go/antlr/v4"

// BasePainlessParserListener is a complete listener for a parse tree produced by PainlessParser.
type BasePainlessParserListener struct{}

var _ PainlessParserListener = &BasePainlessParserListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BasePainlessParserListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BasePainlessParserListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BasePainlessParserListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BasePainlessParserListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterSource is called when production source is entered.
func (s *BasePainlessParserListener) EnterSource(ctx *SourceContext) {}

// ExitSource is called when production source is exited.
func (s *BasePainlessParserListener) ExitSource(ctx *SourceContext) {}

// EnterFunction is called when production function is entered.
func (s *BasePainlessParserListener) EnterFunction(ctx *FunctionContext) {}

// ExitFunction is called when production function is exited.
func (s *BasePainlessParserListener) ExitFunction(ctx *FunctionContext) {}

// EnterParameters is called when production parameters is entered.
func (s *BasePainlessParserListener) EnterParameters(ctx *ParametersContext) {}

// ExitParameters is called when production parameters is exited.
func (s *BasePainlessParserListener) ExitParameters(ctx *ParametersContext) {}

// EnterStatement is called when production statement is entered.
func (s *BasePainlessParserListener) EnterStatement(ctx *StatementContext) {}

// ExitStatement is called when production statement is exited.
func (s *BasePainlessParserListener) ExitStatement(ctx *StatementContext) {}

// EnterIf is called when production if is entered.
func (s *BasePainlessParserListener) EnterIf(ctx *IfContext) {}

// ExitIf is called when production if is exited.
func (s *BasePainlessParserListener) ExitIf(ctx *IfContext) {}

// EnterWhile is called when production while is entered.
func (s *BasePainlessParserListener) EnterWhile(ctx *WhileContext) {}

// ExitWhile is called when production while is exited.
func (s *BasePainlessParserListener) ExitWhile(ctx *WhileContext) {}

// EnterFor is called when production for is entered.
func (s *BasePainlessParserListener) EnterFor(ctx *ForContext) {}

// ExitFor is called when production for is exited.
func (s *BasePainlessParserListener) ExitFor(ctx *ForContext) {}

// EnterEach is called when production each is entered.
func (s *BasePainlessParserListener) EnterEach(ctx *EachContext) {}

// ExitEach is called when production each is exited.
func (s *BasePainlessParserListener) ExitEach(ctx *EachContext) {}

// EnterIneach is called when production ineach is entered.
func (s *BasePainlessParserListener) EnterIneach(ctx *IneachContext) {}

// ExitIneach is called when production ineach is exited.
func (s *BasePainlessParserListener) ExitIneach(ctx *IneachContext) {}

// EnterTry is called when production try is entered.
func (s *BasePainlessParserListener) EnterTry(ctx *TryContext) {}

// ExitTry is called when production try is exited.
func (s *BasePainlessParserListener) ExitTry(ctx *TryContext) {}

// EnterDo is called when production do is entered.
func (s *BasePainlessParserListener) EnterDo(ctx *DoContext) {}

// ExitDo is called when production do is exited.
func (s *BasePainlessParserListener) ExitDo(ctx *DoContext) {}

// EnterDecl is called when production decl is entered.
func (s *BasePainlessParserListener) EnterDecl(ctx *DeclContext) {}

// ExitDecl is called when production decl is exited.
func (s *BasePainlessParserListener) ExitDecl(ctx *DeclContext) {}

// EnterContinue is called when production continue is entered.
func (s *BasePainlessParserListener) EnterContinue(ctx *ContinueContext) {}

// ExitContinue is called when production continue is exited.
func (s *BasePainlessParserListener) ExitContinue(ctx *ContinueContext) {}

// EnterBreak is called when production break is entered.
func (s *BasePainlessParserListener) EnterBreak(ctx *BreakContext) {}

// ExitBreak is called when production break is exited.
func (s *BasePainlessParserListener) ExitBreak(ctx *BreakContext) {}

// EnterReturn is called when production return is entered.
func (s *BasePainlessParserListener) EnterReturn(ctx *ReturnContext) {}

// ExitReturn is called when production return is exited.
func (s *BasePainlessParserListener) ExitReturn(ctx *ReturnContext) {}

// EnterThrow is called when production throw is entered.
func (s *BasePainlessParserListener) EnterThrow(ctx *ThrowContext) {}

// ExitThrow is called when production throw is exited.
func (s *BasePainlessParserListener) ExitThrow(ctx *ThrowContext) {}

// EnterExpr is called when production expr is entered.
func (s *BasePainlessParserListener) EnterExpr(ctx *ExprContext) {}

// ExitExpr is called when production expr is exited.
func (s *BasePainlessParserListener) ExitExpr(ctx *ExprContext) {}

// EnterTrailer is called when production trailer is entered.
func (s *BasePainlessParserListener) EnterTrailer(ctx *TrailerContext) {}

// ExitTrailer is called when production trailer is exited.
func (s *BasePainlessParserListener) ExitTrailer(ctx *TrailerContext) {}

// EnterBlock is called when production block is entered.
func (s *BasePainlessParserListener) EnterBlock(ctx *BlockContext) {}

// ExitBlock is called when production block is exited.
func (s *BasePainlessParserListener) ExitBlock(ctx *BlockContext) {}

// EnterEmpty is called when production empty is entered.
func (s *BasePainlessParserListener) EnterEmpty(ctx *EmptyContext) {}

// ExitEmpty is called when production empty is exited.
func (s *BasePainlessParserListener) ExitEmpty(ctx *EmptyContext) {}

// EnterInitializer is called when production initializer is entered.
func (s *BasePainlessParserListener) EnterInitializer(ctx *InitializerContext) {}

// ExitInitializer is called when production initializer is exited.
func (s *BasePainlessParserListener) ExitInitializer(ctx *InitializerContext) {}

// EnterAfterthought is called when production afterthought is entered.
func (s *BasePainlessParserListener) EnterAfterthought(ctx *AfterthoughtContext) {}

// ExitAfterthought is called when production afterthought is exited.
func (s *BasePainlessParserListener) ExitAfterthought(ctx *AfterthoughtContext) {}

// EnterDeclaration is called when production declaration is entered.
func (s *BasePainlessParserListener) EnterDeclaration(ctx *DeclarationContext) {}

// ExitDeclaration is called when production declaration is exited.
func (s *BasePainlessParserListener) ExitDeclaration(ctx *DeclarationContext) {}

// EnterDecltype is called when production decltype is entered.
func (s *BasePainlessParserListener) EnterDecltype(ctx *DecltypeContext) {}

// ExitDecltype is called when production decltype is exited.
func (s *BasePainlessParserListener) ExitDecltype(ctx *DecltypeContext) {}

// EnterType is called when production type is entered.
func (s *BasePainlessParserListener) EnterType(ctx *TypeContext) {}

// ExitType is called when production type is exited.
func (s *BasePainlessParserListener) ExitType(ctx *TypeContext) {}

// EnterDeclvar is called when production declvar is entered.
func (s *BasePainlessParserListener) EnterDeclvar(ctx *DeclvarContext) {}

// ExitDeclvar is called when production declvar is exited.
func (s *BasePainlessParserListener) ExitDeclvar(ctx *DeclvarContext) {}

// EnterTrap is called when production trap is entered.
func (s *BasePainlessParserListener) EnterTrap(ctx *TrapContext) {}

// ExitTrap is called when production trap is exited.
func (s *BasePainlessParserListener) ExitTrap(ctx *TrapContext) {}

// EnterSingle is called when production single is entered.
func (s *BasePainlessParserListener) EnterSingle(ctx *SingleContext) {}

// ExitSingle is called when production single is exited.
func (s *BasePainlessParserListener) ExitSingle(ctx *SingleContext) {}

// EnterComp is called when production comp is entered.
func (s *BasePainlessParserListener) EnterComp(ctx *CompContext) {}

// ExitComp is called when production comp is exited.
func (s *BasePainlessParserListener) ExitComp(ctx *CompContext) {}

// EnterBool is called when production bool is entered.
func (s *BasePainlessParserListener) EnterBool(ctx *BoolContext) {}

// ExitBool is called when production bool is exited.
func (s *BasePainlessParserListener) ExitBool(ctx *BoolContext) {}

// EnterBinary is called when production binary is entered.
func (s *BasePainlessParserListener) EnterBinary(ctx *BinaryContext) {}

// ExitBinary is called when production binary is exited.
func (s *BasePainlessParserListener) ExitBinary(ctx *BinaryContext) {}

// EnterElvis is called when production elvis is entered.
func (s *BasePainlessParserListener) EnterElvis(ctx *ElvisContext) {}

// ExitElvis is called when production elvis is exited.
func (s *BasePainlessParserListener) ExitElvis(ctx *ElvisContext) {}

// EnterInstanceof is called when production instanceof is entered.
func (s *BasePainlessParserListener) EnterInstanceof(ctx *InstanceofContext) {}

// ExitInstanceof is called when production instanceof is exited.
func (s *BasePainlessParserListener) ExitInstanceof(ctx *InstanceofContext) {}

// EnterNonconditional is called when production nonconditional is entered.
func (s *BasePainlessParserListener) EnterNonconditional(ctx *NonconditionalContext) {}

// ExitNonconditional is called when production nonconditional is exited.
func (s *BasePainlessParserListener) ExitNonconditional(ctx *NonconditionalContext) {}

// EnterConditional is called when production conditional is entered.
func (s *BasePainlessParserListener) EnterConditional(ctx *ConditionalContext) {}

// ExitConditional is called when production conditional is exited.
func (s *BasePainlessParserListener) ExitConditional(ctx *ConditionalContext) {}

// EnterAssignment is called when production assignment is entered.
func (s *BasePainlessParserListener) EnterAssignment(ctx *AssignmentContext) {}

// ExitAssignment is called when production assignment is exited.
func (s *BasePainlessParserListener) ExitAssignment(ctx *AssignmentContext) {}

// EnterPre is called when production pre is entered.
func (s *BasePainlessParserListener) EnterPre(ctx *PreContext) {}

// ExitPre is called when production pre is exited.
func (s *BasePainlessParserListener) ExitPre(ctx *PreContext) {}

// EnterAddsub is called when production addsub is entered.
func (s *BasePainlessParserListener) EnterAddsub(ctx *AddsubContext) {}

// ExitAddsub is called when production addsub is exited.
func (s *BasePainlessParserListener) ExitAddsub(ctx *AddsubContext) {}

// EnterNotaddsub is called when production notaddsub is entered.
func (s *BasePainlessParserListener) EnterNotaddsub(ctx *NotaddsubContext) {}

// ExitNotaddsub is called when production notaddsub is exited.
func (s *BasePainlessParserListener) ExitNotaddsub(ctx *NotaddsubContext) {}

// EnterRead is called when production read is entered.
func (s *BasePainlessParserListener) EnterRead(ctx *ReadContext) {}

// ExitRead is called when production read is exited.
func (s *BasePainlessParserListener) ExitRead(ctx *ReadContext) {}

// EnterPost is called when production post is entered.
func (s *BasePainlessParserListener) EnterPost(ctx *PostContext) {}

// ExitPost is called when production post is exited.
func (s *BasePainlessParserListener) ExitPost(ctx *PostContext) {}

// EnterNot is called when production not is entered.
func (s *BasePainlessParserListener) EnterNot(ctx *NotContext) {}

// ExitNot is called when production not is exited.
func (s *BasePainlessParserListener) ExitNot(ctx *NotContext) {}

// EnterCast is called when production cast is entered.
func (s *BasePainlessParserListener) EnterCast(ctx *CastContext) {}

// ExitCast is called when production cast is exited.
func (s *BasePainlessParserListener) ExitCast(ctx *CastContext) {}

// EnterPrimordefcast is called when production primordefcast is entered.
func (s *BasePainlessParserListener) EnterPrimordefcast(ctx *PrimordefcastContext) {}

// ExitPrimordefcast is called when production primordefcast is exited.
func (s *BasePainlessParserListener) ExitPrimordefcast(ctx *PrimordefcastContext) {}

// EnterRefcast is called when production refcast is entered.
func (s *BasePainlessParserListener) EnterRefcast(ctx *RefcastContext) {}

// ExitRefcast is called when production refcast is exited.
func (s *BasePainlessParserListener) ExitRefcast(ctx *RefcastContext) {}

// EnterPrimordefcasttype is called when production primordefcasttype is entered.
func (s *BasePainlessParserListener) EnterPrimordefcasttype(ctx *PrimordefcasttypeContext) {}

// ExitPrimordefcasttype is called when production primordefcasttype is exited.
func (s *BasePainlessParserListener) ExitPrimordefcasttype(ctx *PrimordefcasttypeContext) {}

// EnterRefcasttype is called when production refcasttype is entered.
func (s *BasePainlessParserListener) EnterRefcasttype(ctx *RefcasttypeContext) {}

// ExitRefcasttype is called when production refcasttype is exited.
func (s *BasePainlessParserListener) ExitRefcasttype(ctx *RefcasttypeContext) {}

// EnterDynamic is called when production dynamic is entered.
func (s *BasePainlessParserListener) EnterDynamic(ctx *DynamicContext) {}

// ExitDynamic is called when production dynamic is exited.
func (s *BasePainlessParserListener) ExitDynamic(ctx *DynamicContext) {}

// EnterNewarray is called when production newarray is entered.
func (s *BasePainlessParserListener) EnterNewarray(ctx *NewarrayContext) {}

// ExitNewarray is called when production newarray is exited.
func (s *BasePainlessParserListener) ExitNewarray(ctx *NewarrayContext) {}

// EnterPrecedence is called when production precedence is entered.
func (s *BasePainlessParserListener) EnterPrecedence(ctx *PrecedenceContext) {}

// ExitPrecedence is called when production precedence is exited.
func (s *BasePainlessParserListener) ExitPrecedence(ctx *PrecedenceContext) {}

// EnterNumeric is called when production numeric is entered.
func (s *BasePainlessParserListener) EnterNumeric(ctx *NumericContext) {}

// ExitNumeric is called when production numeric is exited.
func (s *BasePainlessParserListener) ExitNumeric(ctx *NumericContext) {}

// EnterTrue is called when production true is entered.
func (s *BasePainlessParserListener) EnterTrue(ctx *TrueContext) {}

// ExitTrue is called when production true is exited.
func (s *BasePainlessParserListener) ExitTrue(ctx *TrueContext) {}

// EnterFalse is called when production false is entered.
func (s *BasePainlessParserListener) EnterFalse(ctx *FalseContext) {}

// ExitFalse is called when production false is exited.
func (s *BasePainlessParserListener) ExitFalse(ctx *FalseContext) {}

// EnterNull is called when production null is entered.
func (s *BasePainlessParserListener) EnterNull(ctx *NullContext) {}

// ExitNull is called when production null is exited.
func (s *BasePainlessParserListener) ExitNull(ctx *NullContext) {}

// EnterString is called when production string is entered.
func (s *BasePainlessParserListener) EnterString(ctx *StringContext) {}

// ExitString is called when production string is exited.
func (s *BasePainlessParserListener) ExitString(ctx *StringContext) {}

// EnterRegex is called when production regex is entered.
func (s *BasePainlessParserListener) EnterRegex(ctx *RegexContext) {}

// ExitRegex is called when production regex is exited.
func (s *BasePainlessParserListener) ExitRegex(ctx *RegexContext) {}

// EnterListinit is called when production listinit is entered.
func (s *BasePainlessParserListener) EnterListinit(ctx *ListinitContext) {}

// ExitListinit is called when production listinit is exited.
func (s *BasePainlessParserListener) ExitListinit(ctx *ListinitContext) {}

// EnterMapinit is called when production mapinit is entered.
func (s *BasePainlessParserListener) EnterMapinit(ctx *MapinitContext) {}

// ExitMapinit is called when production mapinit is exited.
func (s *BasePainlessParserListener) ExitMapinit(ctx *MapinitContext) {}

// EnterVariable is called when production variable is entered.
func (s *BasePainlessParserListener) EnterVariable(ctx *VariableContext) {}

// ExitVariable is called when production variable is exited.
func (s *BasePainlessParserListener) ExitVariable(ctx *VariableContext) {}

// EnterCalllocal is called when production calllocal is entered.
func (s *BasePainlessParserListener) EnterCalllocal(ctx *CalllocalContext) {}

// ExitCalllocal is called when production calllocal is exited.
func (s *BasePainlessParserListener) ExitCalllocal(ctx *CalllocalContext) {}

// EnterNewobject is called when production newobject is entered.
func (s *BasePainlessParserListener) EnterNewobject(ctx *NewobjectContext) {}

// ExitNewobject is called when production newobject is exited.
func (s *BasePainlessParserListener) ExitNewobject(ctx *NewobjectContext) {}

// EnterPostfix is called when production postfix is entered.
func (s *BasePainlessParserListener) EnterPostfix(ctx *PostfixContext) {}

// ExitPostfix is called when production postfix is exited.
func (s *BasePainlessParserListener) ExitPostfix(ctx *PostfixContext) {}

// EnterPostdot is called when production postdot is entered.
func (s *BasePainlessParserListener) EnterPostdot(ctx *PostdotContext) {}

// ExitPostdot is called when production postdot is exited.
func (s *BasePainlessParserListener) ExitPostdot(ctx *PostdotContext) {}

// EnterCallinvoke is called when production callinvoke is entered.
func (s *BasePainlessParserListener) EnterCallinvoke(ctx *CallinvokeContext) {}

// ExitCallinvoke is called when production callinvoke is exited.
func (s *BasePainlessParserListener) ExitCallinvoke(ctx *CallinvokeContext) {}

// EnterFieldaccess is called when production fieldaccess is entered.
func (s *BasePainlessParserListener) EnterFieldaccess(ctx *FieldaccessContext) {}

// ExitFieldaccess is called when production fieldaccess is exited.
func (s *BasePainlessParserListener) ExitFieldaccess(ctx *FieldaccessContext) {}

// EnterBraceaccess is called when production braceaccess is entered.
func (s *BasePainlessParserListener) EnterBraceaccess(ctx *BraceaccessContext) {}

// ExitBraceaccess is called when production braceaccess is exited.
func (s *BasePainlessParserListener) ExitBraceaccess(ctx *BraceaccessContext) {}

// EnterNewstandardarray is called when production newstandardarray is entered.
func (s *BasePainlessParserListener) EnterNewstandardarray(ctx *NewstandardarrayContext) {}

// ExitNewstandardarray is called when production newstandardarray is exited.
func (s *BasePainlessParserListener) ExitNewstandardarray(ctx *NewstandardarrayContext) {}

// EnterNewinitializedarray is called when production newinitializedarray is entered.
func (s *BasePainlessParserListener) EnterNewinitializedarray(ctx *NewinitializedarrayContext) {}

// ExitNewinitializedarray is called when production newinitializedarray is exited.
func (s *BasePainlessParserListener) ExitNewinitializedarray(ctx *NewinitializedarrayContext) {}

// EnterListinitializer is called when production listinitializer is entered.
func (s *BasePainlessParserListener) EnterListinitializer(ctx *ListinitializerContext) {}

// ExitListinitializer is called when production listinitializer is exited.
func (s *BasePainlessParserListener) ExitListinitializer(ctx *ListinitializerContext) {}

// EnterMapinitializer is called when production mapinitializer is entered.
func (s *BasePainlessParserListener) EnterMapinitializer(ctx *MapinitializerContext) {}

// ExitMapinitializer is called when production mapinitializer is exited.
func (s *BasePainlessParserListener) ExitMapinitializer(ctx *MapinitializerContext) {}

// EnterMaptoken is called when production maptoken is entered.
func (s *BasePainlessParserListener) EnterMaptoken(ctx *MaptokenContext) {}

// ExitMaptoken is called when production maptoken is exited.
func (s *BasePainlessParserListener) ExitMaptoken(ctx *MaptokenContext) {}

// EnterArguments is called when production arguments is entered.
func (s *BasePainlessParserListener) EnterArguments(ctx *ArgumentsContext) {}

// ExitArguments is called when production arguments is exited.
func (s *BasePainlessParserListener) ExitArguments(ctx *ArgumentsContext) {}

// EnterArgument is called when production argument is entered.
func (s *BasePainlessParserListener) EnterArgument(ctx *ArgumentContext) {}

// ExitArgument is called when production argument is exited.
func (s *BasePainlessParserListener) ExitArgument(ctx *ArgumentContext) {}

// EnterLambda is called when production lambda is entered.
func (s *BasePainlessParserListener) EnterLambda(ctx *LambdaContext) {}

// ExitLambda is called when production lambda is exited.
func (s *BasePainlessParserListener) ExitLambda(ctx *LambdaContext) {}

// EnterLamtype is called when production lamtype is entered.
func (s *BasePainlessParserListener) EnterLamtype(ctx *LamtypeContext) {}

// ExitLamtype is called when production lamtype is exited.
func (s *BasePainlessParserListener) ExitLamtype(ctx *LamtypeContext) {}

// EnterClassfuncref is called when production classfuncref is entered.
func (s *BasePainlessParserListener) EnterClassfuncref(ctx *ClassfuncrefContext) {}

// ExitClassfuncref is called when production classfuncref is exited.
func (s *BasePainlessParserListener) ExitClassfuncref(ctx *ClassfuncrefContext) {}

// EnterConstructorfuncref is called when production constructorfuncref is entered.
func (s *BasePainlessParserListener) EnterConstructorfuncref(ctx *ConstructorfuncrefContext) {}

// ExitConstructorfuncref is called when production constructorfuncref is exited.
func (s *BasePainlessParserListener) ExitConstructorfuncref(ctx *ConstructorfuncrefContext) {}

// EnterLocalfuncref is called when production localfuncref is entered.
func (s *BasePainlessParserListener) EnterLocalfuncref(ctx *LocalfuncrefContext) {}

// ExitLocalfuncref is called when production localfuncref is exited.
func (s *BasePainlessParserListener) ExitLocalfuncref(ctx *LocalfuncrefContext) {}
