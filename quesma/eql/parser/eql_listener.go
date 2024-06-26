// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
// Code generated from quesma/eql/parser/EQL.g4 by ANTLR 4.13.1. DO NOT EDIT.

package parser // EQL
import "github.com/antlr4-go/antlr/v4"

// EQLListener is a complete listener for a parse tree produced by EQLParser.
type EQLListener interface {
	antlr.ParseTreeListener

	// EnterQuery is called when entering the query production.
	EnterQuery(c *QueryContext)

	// EnterSimpleQuery is called when entering the simpleQuery production.
	EnterSimpleQuery(c *SimpleQueryContext)

	// EnterSequenceQuery is called when entering the sequenceQuery production.
	EnterSequenceQuery(c *SequenceQueryContext)

	// EnterSampleQuery is called when entering the sampleQuery production.
	EnterSampleQuery(c *SampleQueryContext)

	// EnterLookupOpList is called when entering the LookupOpList production.
	EnterLookupOpList(c *LookupOpListContext)

	// EnterComparisonOp is called when entering the ComparisonOp production.
	EnterComparisonOp(c *ComparisonOpContext)

	// EnterConditionNotFuncall is called when entering the ConditionNotFuncall production.
	EnterConditionNotFuncall(c *ConditionNotFuncallContext)

	// EnterConditionBoolean is called when entering the ConditionBoolean production.
	EnterConditionBoolean(c *ConditionBooleanContext)

	// EnterConditionNot is called when entering the ConditionNot production.
	EnterConditionNot(c *ConditionNotContext)

	// EnterLookupNotOpList is called when entering the LookupNotOpList production.
	EnterLookupNotOpList(c *LookupNotOpListContext)

	// EnterConditionLogicalOp is called when entering the ConditionLogicalOp production.
	EnterConditionLogicalOp(c *ConditionLogicalOpContext)

	// EnterConditionGroup is called when entering the ConditionGroup production.
	EnterConditionGroup(c *ConditionGroupContext)

	// EnterConditionFuncall is called when entering the ConditionFuncall production.
	EnterConditionFuncall(c *ConditionFuncallContext)

	// EnterCategory is called when entering the category production.
	EnterCategory(c *CategoryContext)

	// EnterField is called when entering the field production.
	EnterField(c *FieldContext)

	// EnterFieldList is called when entering the fieldList production.
	EnterFieldList(c *FieldListContext)

	// EnterLiteral is called when entering the literal production.
	EnterLiteral(c *LiteralContext)

	// EnterLiteralList is called when entering the literalList production.
	EnterLiteralList(c *LiteralListContext)

	// EnterValueAddSub is called when entering the ValueAddSub production.
	EnterValueAddSub(c *ValueAddSubContext)

	// EnterValueNull is called when entering the ValueNull production.
	EnterValueNull(c *ValueNullContext)

	// EnterValueMulDiv is called when entering the ValueMulDiv production.
	EnterValueMulDiv(c *ValueMulDivContext)

	// EnterValueGroup is called when entering the ValueGroup production.
	EnterValueGroup(c *ValueGroupContext)

	// EnterValueLiteral is called when entering the ValueLiteral production.
	EnterValueLiteral(c *ValueLiteralContext)

	// EnterValueFuncall is called when entering the ValueFuncall production.
	EnterValueFuncall(c *ValueFuncallContext)

	// EnterValueField is called when entering the ValueField production.
	EnterValueField(c *ValueFieldContext)

	// EnterPipeHead is called when entering the PipeHead production.
	EnterPipeHead(c *PipeHeadContext)

	// EnterPipeTail is called when entering the PipeTail production.
	EnterPipeTail(c *PipeTailContext)

	// EnterPipeCount is called when entering the PipeCount production.
	EnterPipeCount(c *PipeCountContext)

	// EnterPipeUnique is called when entering the PipeUnique production.
	EnterPipeUnique(c *PipeUniqueContext)

	// EnterPipeFilter is called when entering the PipeFilter production.
	EnterPipeFilter(c *PipeFilterContext)

	// EnterPipeSort is called when entering the PipeSort production.
	EnterPipeSort(c *PipeSortContext)

	// EnterFuncall is called when entering the funcall production.
	EnterFuncall(c *FuncallContext)

	// EnterFuncName is called when entering the funcName production.
	EnterFuncName(c *FuncNameContext)

	// EnterInterval is called when entering the interval production.
	EnterInterval(c *IntervalContext)

	// ExitQuery is called when exiting the query production.
	ExitQuery(c *QueryContext)

	// ExitSimpleQuery is called when exiting the simpleQuery production.
	ExitSimpleQuery(c *SimpleQueryContext)

	// ExitSequenceQuery is called when exiting the sequenceQuery production.
	ExitSequenceQuery(c *SequenceQueryContext)

	// ExitSampleQuery is called when exiting the sampleQuery production.
	ExitSampleQuery(c *SampleQueryContext)

	// ExitLookupOpList is called when exiting the LookupOpList production.
	ExitLookupOpList(c *LookupOpListContext)

	// ExitComparisonOp is called when exiting the ComparisonOp production.
	ExitComparisonOp(c *ComparisonOpContext)

	// ExitConditionNotFuncall is called when exiting the ConditionNotFuncall production.
	ExitConditionNotFuncall(c *ConditionNotFuncallContext)

	// ExitConditionBoolean is called when exiting the ConditionBoolean production.
	ExitConditionBoolean(c *ConditionBooleanContext)

	// ExitConditionNot is called when exiting the ConditionNot production.
	ExitConditionNot(c *ConditionNotContext)

	// ExitLookupNotOpList is called when exiting the LookupNotOpList production.
	ExitLookupNotOpList(c *LookupNotOpListContext)

	// ExitConditionLogicalOp is called when exiting the ConditionLogicalOp production.
	ExitConditionLogicalOp(c *ConditionLogicalOpContext)

	// ExitConditionGroup is called when exiting the ConditionGroup production.
	ExitConditionGroup(c *ConditionGroupContext)

	// ExitConditionFuncall is called when exiting the ConditionFuncall production.
	ExitConditionFuncall(c *ConditionFuncallContext)

	// ExitCategory is called when exiting the category production.
	ExitCategory(c *CategoryContext)

	// ExitField is called when exiting the field production.
	ExitField(c *FieldContext)

	// ExitFieldList is called when exiting the fieldList production.
	ExitFieldList(c *FieldListContext)

	// ExitLiteral is called when exiting the literal production.
	ExitLiteral(c *LiteralContext)

	// ExitLiteralList is called when exiting the literalList production.
	ExitLiteralList(c *LiteralListContext)

	// ExitValueAddSub is called when exiting the ValueAddSub production.
	ExitValueAddSub(c *ValueAddSubContext)

	// ExitValueNull is called when exiting the ValueNull production.
	ExitValueNull(c *ValueNullContext)

	// ExitValueMulDiv is called when exiting the ValueMulDiv production.
	ExitValueMulDiv(c *ValueMulDivContext)

	// ExitValueGroup is called when exiting the ValueGroup production.
	ExitValueGroup(c *ValueGroupContext)

	// ExitValueLiteral is called when exiting the ValueLiteral production.
	ExitValueLiteral(c *ValueLiteralContext)

	// ExitValueFuncall is called when exiting the ValueFuncall production.
	ExitValueFuncall(c *ValueFuncallContext)

	// ExitValueField is called when exiting the ValueField production.
	ExitValueField(c *ValueFieldContext)

	// ExitPipeHead is called when exiting the PipeHead production.
	ExitPipeHead(c *PipeHeadContext)

	// ExitPipeTail is called when exiting the PipeTail production.
	ExitPipeTail(c *PipeTailContext)

	// ExitPipeCount is called when exiting the PipeCount production.
	ExitPipeCount(c *PipeCountContext)

	// ExitPipeUnique is called when exiting the PipeUnique production.
	ExitPipeUnique(c *PipeUniqueContext)

	// ExitPipeFilter is called when exiting the PipeFilter production.
	ExitPipeFilter(c *PipeFilterContext)

	// ExitPipeSort is called when exiting the PipeSort production.
	ExitPipeSort(c *PipeSortContext)

	// ExitFuncall is called when exiting the funcall production.
	ExitFuncall(c *FuncallContext)

	// ExitFuncName is called when exiting the funcName production.
	ExitFuncName(c *FuncNameContext)

	// ExitInterval is called when exiting the interval production.
	ExitInterval(c *IntervalContext)
}
