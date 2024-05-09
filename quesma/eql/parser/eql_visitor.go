// Code generated from quesma/eql/parser/EQL.g4 by ANTLR 4.13.1. DO NOT EDIT.

package parser // EQL
import "github.com/antlr4-go/antlr/v4"

// A complete Visitor for a parse tree produced by EQLParser.
type EQLVisitor interface {
	antlr.ParseTreeVisitor

	// Visit a parse tree produced by EQLParser#query.
	VisitQuery(ctx *QueryContext) interface{}

	// Visit a parse tree produced by EQLParser#simpleQuery.
	VisitSimpleQuery(ctx *SimpleQueryContext) interface{}

	// Visit a parse tree produced by EQLParser#sequenceQuery.
	VisitSequenceQuery(ctx *SequenceQueryContext) interface{}

	// Visit a parse tree produced by EQLParser#sampleQuery.
	VisitSampleQuery(ctx *SampleQueryContext) interface{}

	// Visit a parse tree produced by EQLParser#LookupOpList.
	VisitLookupOpList(ctx *LookupOpListContext) interface{}

	// Visit a parse tree produced by EQLParser#ComparisonOp.
	VisitComparisonOp(ctx *ComparisonOpContext) interface{}

	// Visit a parse tree produced by EQLParser#ConditionNotFuncall.
	VisitConditionNotFuncall(ctx *ConditionNotFuncallContext) interface{}

	// Visit a parse tree produced by EQLParser#ConditionBoolean.
	VisitConditionBoolean(ctx *ConditionBooleanContext) interface{}

	// Visit a parse tree produced by EQLParser#ConditionNot.
	VisitConditionNot(ctx *ConditionNotContext) interface{}

	// Visit a parse tree produced by EQLParser#LookupNotOpList.
	VisitLookupNotOpList(ctx *LookupNotOpListContext) interface{}

	// Visit a parse tree produced by EQLParser#ConditionLogicalOp.
	VisitConditionLogicalOp(ctx *ConditionLogicalOpContext) interface{}

	// Visit a parse tree produced by EQLParser#ConditionGroup.
	VisitConditionGroup(ctx *ConditionGroupContext) interface{}

	// Visit a parse tree produced by EQLParser#ConditionFuncall.
	VisitConditionFuncall(ctx *ConditionFuncallContext) interface{}

	// Visit a parse tree produced by EQLParser#category.
	VisitCategory(ctx *CategoryContext) interface{}

	// Visit a parse tree produced by EQLParser#field.
	VisitField(ctx *FieldContext) interface{}

	// Visit a parse tree produced by EQLParser#fieldList.
	VisitFieldList(ctx *FieldListContext) interface{}

	// Visit a parse tree produced by EQLParser#literal.
	VisitLiteral(ctx *LiteralContext) interface{}

	// Visit a parse tree produced by EQLParser#literalList.
	VisitLiteralList(ctx *LiteralListContext) interface{}

	// Visit a parse tree produced by EQLParser#ValueAddSub.
	VisitValueAddSub(ctx *ValueAddSubContext) interface{}

	// Visit a parse tree produced by EQLParser#ValueNull.
	VisitValueNull(ctx *ValueNullContext) interface{}

	// Visit a parse tree produced by EQLParser#ValueMulDiv.
	VisitValueMulDiv(ctx *ValueMulDivContext) interface{}

	// Visit a parse tree produced by EQLParser#ValueGroup.
	VisitValueGroup(ctx *ValueGroupContext) interface{}

	// Visit a parse tree produced by EQLParser#ValueLiteral.
	VisitValueLiteral(ctx *ValueLiteralContext) interface{}

	// Visit a parse tree produced by EQLParser#ValueFuncall.
	VisitValueFuncall(ctx *ValueFuncallContext) interface{}

	// Visit a parse tree produced by EQLParser#ValueField.
	VisitValueField(ctx *ValueFieldContext) interface{}

	// Visit a parse tree produced by EQLParser#funcall.
	VisitFuncall(ctx *FuncallContext) interface{}

	// Visit a parse tree produced by EQLParser#funcName.
	VisitFuncName(ctx *FuncNameContext) interface{}

	// Visit a parse tree produced by EQLParser#interval.
	VisitInterval(ctx *IntervalContext) interface{}
}
