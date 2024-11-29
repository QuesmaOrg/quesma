// Code generated from quesma/queryparser/painless/antlr/PainlessParser.g4 by ANTLR 4.13.2. DO NOT EDIT.

package parser // PainlessParser
import "github.com/antlr4-go/antlr/v4"

// PainlessParserListener is a complete listener for a parse tree produced by PainlessParser.
type PainlessParserListener interface {
	antlr.ParseTreeListener

	// EnterSource is called when entering the source production.
	EnterSource(c *SourceContext)

	// EnterFunction is called when entering the function production.
	EnterFunction(c *FunctionContext)

	// EnterParameters is called when entering the parameters production.
	EnterParameters(c *ParametersContext)

	// EnterStatement is called when entering the statement production.
	EnterStatement(c *StatementContext)

	// EnterIf is called when entering the if production.
	EnterIf(c *IfContext)

	// EnterWhile is called when entering the while production.
	EnterWhile(c *WhileContext)

	// EnterFor is called when entering the for production.
	EnterFor(c *ForContext)

	// EnterEach is called when entering the each production.
	EnterEach(c *EachContext)

	// EnterIneach is called when entering the ineach production.
	EnterIneach(c *IneachContext)

	// EnterTry is called when entering the try production.
	EnterTry(c *TryContext)

	// EnterDo is called when entering the do production.
	EnterDo(c *DoContext)

	// EnterDecl is called when entering the decl production.
	EnterDecl(c *DeclContext)

	// EnterContinue is called when entering the continue production.
	EnterContinue(c *ContinueContext)

	// EnterBreak is called when entering the break production.
	EnterBreak(c *BreakContext)

	// EnterReturn is called when entering the return production.
	EnterReturn(c *ReturnContext)

	// EnterThrow is called when entering the throw production.
	EnterThrow(c *ThrowContext)

	// EnterExpr is called when entering the expr production.
	EnterExpr(c *ExprContext)

	// EnterTrailer is called when entering the trailer production.
	EnterTrailer(c *TrailerContext)

	// EnterBlock is called when entering the block production.
	EnterBlock(c *BlockContext)

	// EnterEmpty is called when entering the empty production.
	EnterEmpty(c *EmptyContext)

	// EnterInitializer is called when entering the initializer production.
	EnterInitializer(c *InitializerContext)

	// EnterAfterthought is called when entering the afterthought production.
	EnterAfterthought(c *AfterthoughtContext)

	// EnterDeclaration is called when entering the declaration production.
	EnterDeclaration(c *DeclarationContext)

	// EnterDecltype is called when entering the decltype production.
	EnterDecltype(c *DecltypeContext)

	// EnterType is called when entering the type production.
	EnterType(c *TypeContext)

	// EnterDeclvar is called when entering the declvar production.
	EnterDeclvar(c *DeclvarContext)

	// EnterTrap is called when entering the trap production.
	EnterTrap(c *TrapContext)

	// EnterSingle is called when entering the single production.
	EnterSingle(c *SingleContext)

	// EnterComp is called when entering the comp production.
	EnterComp(c *CompContext)

	// EnterBool is called when entering the bool production.
	EnterBool(c *BoolContext)

	// EnterBinary is called when entering the binary production.
	EnterBinary(c *BinaryContext)

	// EnterElvis is called when entering the elvis production.
	EnterElvis(c *ElvisContext)

	// EnterInstanceof is called when entering the instanceof production.
	EnterInstanceof(c *InstanceofContext)

	// EnterNonconditional is called when entering the nonconditional production.
	EnterNonconditional(c *NonconditionalContext)

	// EnterConditional is called when entering the conditional production.
	EnterConditional(c *ConditionalContext)

	// EnterAssignment is called when entering the assignment production.
	EnterAssignment(c *AssignmentContext)

	// EnterPre is called when entering the pre production.
	EnterPre(c *PreContext)

	// EnterAddsub is called when entering the addsub production.
	EnterAddsub(c *AddsubContext)

	// EnterNotaddsub is called when entering the notaddsub production.
	EnterNotaddsub(c *NotaddsubContext)

	// EnterRead is called when entering the read production.
	EnterRead(c *ReadContext)

	// EnterPost is called when entering the post production.
	EnterPost(c *PostContext)

	// EnterNot is called when entering the not production.
	EnterNot(c *NotContext)

	// EnterCast is called when entering the cast production.
	EnterCast(c *CastContext)

	// EnterPrimordefcast is called when entering the primordefcast production.
	EnterPrimordefcast(c *PrimordefcastContext)

	// EnterRefcast is called when entering the refcast production.
	EnterRefcast(c *RefcastContext)

	// EnterPrimordefcasttype is called when entering the primordefcasttype production.
	EnterPrimordefcasttype(c *PrimordefcasttypeContext)

	// EnterRefcasttype is called when entering the refcasttype production.
	EnterRefcasttype(c *RefcasttypeContext)

	// EnterDynamic is called when entering the dynamic production.
	EnterDynamic(c *DynamicContext)

	// EnterNewarray is called when entering the newarray production.
	EnterNewarray(c *NewarrayContext)

	// EnterPrecedence is called when entering the precedence production.
	EnterPrecedence(c *PrecedenceContext)

	// EnterNumeric is called when entering the numeric production.
	EnterNumeric(c *NumericContext)

	// EnterTrue is called when entering the true production.
	EnterTrue(c *TrueContext)

	// EnterFalse is called when entering the false production.
	EnterFalse(c *FalseContext)

	// EnterNull is called when entering the null production.
	EnterNull(c *NullContext)

	// EnterString is called when entering the string production.
	EnterString(c *StringContext)

	// EnterRegex is called when entering the regex production.
	EnterRegex(c *RegexContext)

	// EnterListinit is called when entering the listinit production.
	EnterListinit(c *ListinitContext)

	// EnterMapinit is called when entering the mapinit production.
	EnterMapinit(c *MapinitContext)

	// EnterVariable is called when entering the variable production.
	EnterVariable(c *VariableContext)

	// EnterCalllocal is called when entering the calllocal production.
	EnterCalllocal(c *CalllocalContext)

	// EnterNewobject is called when entering the newobject production.
	EnterNewobject(c *NewobjectContext)

	// EnterPostfix is called when entering the postfix production.
	EnterPostfix(c *PostfixContext)

	// EnterPostdot is called when entering the postdot production.
	EnterPostdot(c *PostdotContext)

	// EnterCallinvoke is called when entering the callinvoke production.
	EnterCallinvoke(c *CallinvokeContext)

	// EnterFieldaccess is called when entering the fieldaccess production.
	EnterFieldaccess(c *FieldaccessContext)

	// EnterBraceaccess is called when entering the braceaccess production.
	EnterBraceaccess(c *BraceaccessContext)

	// EnterNewstandardarray is called when entering the newstandardarray production.
	EnterNewstandardarray(c *NewstandardarrayContext)

	// EnterNewinitializedarray is called when entering the newinitializedarray production.
	EnterNewinitializedarray(c *NewinitializedarrayContext)

	// EnterListinitializer is called when entering the listinitializer production.
	EnterListinitializer(c *ListinitializerContext)

	// EnterMapinitializer is called when entering the mapinitializer production.
	EnterMapinitializer(c *MapinitializerContext)

	// EnterMaptoken is called when entering the maptoken production.
	EnterMaptoken(c *MaptokenContext)

	// EnterArguments is called when entering the arguments production.
	EnterArguments(c *ArgumentsContext)

	// EnterArgument is called when entering the argument production.
	EnterArgument(c *ArgumentContext)

	// EnterLambda is called when entering the lambda production.
	EnterLambda(c *LambdaContext)

	// EnterLamtype is called when entering the lamtype production.
	EnterLamtype(c *LamtypeContext)

	// EnterClassfuncref is called when entering the classfuncref production.
	EnterClassfuncref(c *ClassfuncrefContext)

	// EnterConstructorfuncref is called when entering the constructorfuncref production.
	EnterConstructorfuncref(c *ConstructorfuncrefContext)

	// EnterLocalfuncref is called when entering the localfuncref production.
	EnterLocalfuncref(c *LocalfuncrefContext)

	// ExitSource is called when exiting the source production.
	ExitSource(c *SourceContext)

	// ExitFunction is called when exiting the function production.
	ExitFunction(c *FunctionContext)

	// ExitParameters is called when exiting the parameters production.
	ExitParameters(c *ParametersContext)

	// ExitStatement is called when exiting the statement production.
	ExitStatement(c *StatementContext)

	// ExitIf is called when exiting the if production.
	ExitIf(c *IfContext)

	// ExitWhile is called when exiting the while production.
	ExitWhile(c *WhileContext)

	// ExitFor is called when exiting the for production.
	ExitFor(c *ForContext)

	// ExitEach is called when exiting the each production.
	ExitEach(c *EachContext)

	// ExitIneach is called when exiting the ineach production.
	ExitIneach(c *IneachContext)

	// ExitTry is called when exiting the try production.
	ExitTry(c *TryContext)

	// ExitDo is called when exiting the do production.
	ExitDo(c *DoContext)

	// ExitDecl is called when exiting the decl production.
	ExitDecl(c *DeclContext)

	// ExitContinue is called when exiting the continue production.
	ExitContinue(c *ContinueContext)

	// ExitBreak is called when exiting the break production.
	ExitBreak(c *BreakContext)

	// ExitReturn is called when exiting the return production.
	ExitReturn(c *ReturnContext)

	// ExitThrow is called when exiting the throw production.
	ExitThrow(c *ThrowContext)

	// ExitExpr is called when exiting the expr production.
	ExitExpr(c *ExprContext)

	// ExitTrailer is called when exiting the trailer production.
	ExitTrailer(c *TrailerContext)

	// ExitBlock is called when exiting the block production.
	ExitBlock(c *BlockContext)

	// ExitEmpty is called when exiting the empty production.
	ExitEmpty(c *EmptyContext)

	// ExitInitializer is called when exiting the initializer production.
	ExitInitializer(c *InitializerContext)

	// ExitAfterthought is called when exiting the afterthought production.
	ExitAfterthought(c *AfterthoughtContext)

	// ExitDeclaration is called when exiting the declaration production.
	ExitDeclaration(c *DeclarationContext)

	// ExitDecltype is called when exiting the decltype production.
	ExitDecltype(c *DecltypeContext)

	// ExitType is called when exiting the type production.
	ExitType(c *TypeContext)

	// ExitDeclvar is called when exiting the declvar production.
	ExitDeclvar(c *DeclvarContext)

	// ExitTrap is called when exiting the trap production.
	ExitTrap(c *TrapContext)

	// ExitSingle is called when exiting the single production.
	ExitSingle(c *SingleContext)

	// ExitComp is called when exiting the comp production.
	ExitComp(c *CompContext)

	// ExitBool is called when exiting the bool production.
	ExitBool(c *BoolContext)

	// ExitBinary is called when exiting the binary production.
	ExitBinary(c *BinaryContext)

	// ExitElvis is called when exiting the elvis production.
	ExitElvis(c *ElvisContext)

	// ExitInstanceof is called when exiting the instanceof production.
	ExitInstanceof(c *InstanceofContext)

	// ExitNonconditional is called when exiting the nonconditional production.
	ExitNonconditional(c *NonconditionalContext)

	// ExitConditional is called when exiting the conditional production.
	ExitConditional(c *ConditionalContext)

	// ExitAssignment is called when exiting the assignment production.
	ExitAssignment(c *AssignmentContext)

	// ExitPre is called when exiting the pre production.
	ExitPre(c *PreContext)

	// ExitAddsub is called when exiting the addsub production.
	ExitAddsub(c *AddsubContext)

	// ExitNotaddsub is called when exiting the notaddsub production.
	ExitNotaddsub(c *NotaddsubContext)

	// ExitRead is called when exiting the read production.
	ExitRead(c *ReadContext)

	// ExitPost is called when exiting the post production.
	ExitPost(c *PostContext)

	// ExitNot is called when exiting the not production.
	ExitNot(c *NotContext)

	// ExitCast is called when exiting the cast production.
	ExitCast(c *CastContext)

	// ExitPrimordefcast is called when exiting the primordefcast production.
	ExitPrimordefcast(c *PrimordefcastContext)

	// ExitRefcast is called when exiting the refcast production.
	ExitRefcast(c *RefcastContext)

	// ExitPrimordefcasttype is called when exiting the primordefcasttype production.
	ExitPrimordefcasttype(c *PrimordefcasttypeContext)

	// ExitRefcasttype is called when exiting the refcasttype production.
	ExitRefcasttype(c *RefcasttypeContext)

	// ExitDynamic is called when exiting the dynamic production.
	ExitDynamic(c *DynamicContext)

	// ExitNewarray is called when exiting the newarray production.
	ExitNewarray(c *NewarrayContext)

	// ExitPrecedence is called when exiting the precedence production.
	ExitPrecedence(c *PrecedenceContext)

	// ExitNumeric is called when exiting the numeric production.
	ExitNumeric(c *NumericContext)

	// ExitTrue is called when exiting the true production.
	ExitTrue(c *TrueContext)

	// ExitFalse is called when exiting the false production.
	ExitFalse(c *FalseContext)

	// ExitNull is called when exiting the null production.
	ExitNull(c *NullContext)

	// ExitString is called when exiting the string production.
	ExitString(c *StringContext)

	// ExitRegex is called when exiting the regex production.
	ExitRegex(c *RegexContext)

	// ExitListinit is called when exiting the listinit production.
	ExitListinit(c *ListinitContext)

	// ExitMapinit is called when exiting the mapinit production.
	ExitMapinit(c *MapinitContext)

	// ExitVariable is called when exiting the variable production.
	ExitVariable(c *VariableContext)

	// ExitCalllocal is called when exiting the calllocal production.
	ExitCalllocal(c *CalllocalContext)

	// ExitNewobject is called when exiting the newobject production.
	ExitNewobject(c *NewobjectContext)

	// ExitPostfix is called when exiting the postfix production.
	ExitPostfix(c *PostfixContext)

	// ExitPostdot is called when exiting the postdot production.
	ExitPostdot(c *PostdotContext)

	// ExitCallinvoke is called when exiting the callinvoke production.
	ExitCallinvoke(c *CallinvokeContext)

	// ExitFieldaccess is called when exiting the fieldaccess production.
	ExitFieldaccess(c *FieldaccessContext)

	// ExitBraceaccess is called when exiting the braceaccess production.
	ExitBraceaccess(c *BraceaccessContext)

	// ExitNewstandardarray is called when exiting the newstandardarray production.
	ExitNewstandardarray(c *NewstandardarrayContext)

	// ExitNewinitializedarray is called when exiting the newinitializedarray production.
	ExitNewinitializedarray(c *NewinitializedarrayContext)

	// ExitListinitializer is called when exiting the listinitializer production.
	ExitListinitializer(c *ListinitializerContext)

	// ExitMapinitializer is called when exiting the mapinitializer production.
	ExitMapinitializer(c *MapinitializerContext)

	// ExitMaptoken is called when exiting the maptoken production.
	ExitMaptoken(c *MaptokenContext)

	// ExitArguments is called when exiting the arguments production.
	ExitArguments(c *ArgumentsContext)

	// ExitArgument is called when exiting the argument production.
	ExitArgument(c *ArgumentContext)

	// ExitLambda is called when exiting the lambda production.
	ExitLambda(c *LambdaContext)

	// ExitLamtype is called when exiting the lamtype production.
	ExitLamtype(c *LamtypeContext)

	// ExitClassfuncref is called when exiting the classfuncref production.
	ExitClassfuncref(c *ClassfuncrefContext)

	// ExitConstructorfuncref is called when exiting the constructorfuncref production.
	ExitConstructorfuncref(c *ConstructorfuncrefContext)

	// ExitLocalfuncref is called when exiting the localfuncref production.
	ExitLocalfuncref(c *LocalfuncrefContext)
}
