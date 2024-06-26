// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
// Code generated from quesma/eql/parser/EQL.g4 by ANTLR 4.13.1. DO NOT EDIT.

package parser // EQL
import "github.com/antlr4-go/antlr/v4"

// BaseEQLListener is a complete listener for a parse tree produced by EQLParser.
type BaseEQLListener struct{}

var _ EQLListener = &BaseEQLListener{}

// VisitTerminal is called when a terminal node is visited.
func (s *BaseEQLListener) VisitTerminal(node antlr.TerminalNode) {}

// VisitErrorNode is called when an error node is visited.
func (s *BaseEQLListener) VisitErrorNode(node antlr.ErrorNode) {}

// EnterEveryRule is called when any rule is entered.
func (s *BaseEQLListener) EnterEveryRule(ctx antlr.ParserRuleContext) {}

// ExitEveryRule is called when any rule is exited.
func (s *BaseEQLListener) ExitEveryRule(ctx antlr.ParserRuleContext) {}

// EnterQuery is called when production query is entered.
func (s *BaseEQLListener) EnterQuery(ctx *QueryContext) {}

// ExitQuery is called when production query is exited.
func (s *BaseEQLListener) ExitQuery(ctx *QueryContext) {}

// EnterSimpleQuery is called when production simpleQuery is entered.
func (s *BaseEQLListener) EnterSimpleQuery(ctx *SimpleQueryContext) {}

// ExitSimpleQuery is called when production simpleQuery is exited.
func (s *BaseEQLListener) ExitSimpleQuery(ctx *SimpleQueryContext) {}

// EnterSequenceQuery is called when production sequenceQuery is entered.
func (s *BaseEQLListener) EnterSequenceQuery(ctx *SequenceQueryContext) {}

// ExitSequenceQuery is called when production sequenceQuery is exited.
func (s *BaseEQLListener) ExitSequenceQuery(ctx *SequenceQueryContext) {}

// EnterSampleQuery is called when production sampleQuery is entered.
func (s *BaseEQLListener) EnterSampleQuery(ctx *SampleQueryContext) {}

// ExitSampleQuery is called when production sampleQuery is exited.
func (s *BaseEQLListener) ExitSampleQuery(ctx *SampleQueryContext) {}

// EnterLookupOpList is called when production LookupOpList is entered.
func (s *BaseEQLListener) EnterLookupOpList(ctx *LookupOpListContext) {}

// ExitLookupOpList is called when production LookupOpList is exited.
func (s *BaseEQLListener) ExitLookupOpList(ctx *LookupOpListContext) {}

// EnterComparisonOp is called when production ComparisonOp is entered.
func (s *BaseEQLListener) EnterComparisonOp(ctx *ComparisonOpContext) {}

// ExitComparisonOp is called when production ComparisonOp is exited.
func (s *BaseEQLListener) ExitComparisonOp(ctx *ComparisonOpContext) {}

// EnterConditionNotFuncall is called when production ConditionNotFuncall is entered.
func (s *BaseEQLListener) EnterConditionNotFuncall(ctx *ConditionNotFuncallContext) {}

// ExitConditionNotFuncall is called when production ConditionNotFuncall is exited.
func (s *BaseEQLListener) ExitConditionNotFuncall(ctx *ConditionNotFuncallContext) {}

// EnterConditionBoolean is called when production ConditionBoolean is entered.
func (s *BaseEQLListener) EnterConditionBoolean(ctx *ConditionBooleanContext) {}

// ExitConditionBoolean is called when production ConditionBoolean is exited.
func (s *BaseEQLListener) ExitConditionBoolean(ctx *ConditionBooleanContext) {}

// EnterConditionNot is called when production ConditionNot is entered.
func (s *BaseEQLListener) EnterConditionNot(ctx *ConditionNotContext) {}

// ExitConditionNot is called when production ConditionNot is exited.
func (s *BaseEQLListener) ExitConditionNot(ctx *ConditionNotContext) {}

// EnterLookupNotOpList is called when production LookupNotOpList is entered.
func (s *BaseEQLListener) EnterLookupNotOpList(ctx *LookupNotOpListContext) {}

// ExitLookupNotOpList is called when production LookupNotOpList is exited.
func (s *BaseEQLListener) ExitLookupNotOpList(ctx *LookupNotOpListContext) {}

// EnterConditionLogicalOp is called when production ConditionLogicalOp is entered.
func (s *BaseEQLListener) EnterConditionLogicalOp(ctx *ConditionLogicalOpContext) {}

// ExitConditionLogicalOp is called when production ConditionLogicalOp is exited.
func (s *BaseEQLListener) ExitConditionLogicalOp(ctx *ConditionLogicalOpContext) {}

// EnterConditionGroup is called when production ConditionGroup is entered.
func (s *BaseEQLListener) EnterConditionGroup(ctx *ConditionGroupContext) {}

// ExitConditionGroup is called when production ConditionGroup is exited.
func (s *BaseEQLListener) ExitConditionGroup(ctx *ConditionGroupContext) {}

// EnterConditionFuncall is called when production ConditionFuncall is entered.
func (s *BaseEQLListener) EnterConditionFuncall(ctx *ConditionFuncallContext) {}

// ExitConditionFuncall is called when production ConditionFuncall is exited.
func (s *BaseEQLListener) ExitConditionFuncall(ctx *ConditionFuncallContext) {}

// EnterCategory is called when production category is entered.
func (s *BaseEQLListener) EnterCategory(ctx *CategoryContext) {}

// ExitCategory is called when production category is exited.
func (s *BaseEQLListener) ExitCategory(ctx *CategoryContext) {}

// EnterField is called when production field is entered.
func (s *BaseEQLListener) EnterField(ctx *FieldContext) {}

// ExitField is called when production field is exited.
func (s *BaseEQLListener) ExitField(ctx *FieldContext) {}

// EnterFieldList is called when production fieldList is entered.
func (s *BaseEQLListener) EnterFieldList(ctx *FieldListContext) {}

// ExitFieldList is called when production fieldList is exited.
func (s *BaseEQLListener) ExitFieldList(ctx *FieldListContext) {}

// EnterLiteral is called when production literal is entered.
func (s *BaseEQLListener) EnterLiteral(ctx *LiteralContext) {}

// ExitLiteral is called when production literal is exited.
func (s *BaseEQLListener) ExitLiteral(ctx *LiteralContext) {}

// EnterLiteralList is called when production literalList is entered.
func (s *BaseEQLListener) EnterLiteralList(ctx *LiteralListContext) {}

// ExitLiteralList is called when production literalList is exited.
func (s *BaseEQLListener) ExitLiteralList(ctx *LiteralListContext) {}

// EnterValueAddSub is called when production ValueAddSub is entered.
func (s *BaseEQLListener) EnterValueAddSub(ctx *ValueAddSubContext) {}

// ExitValueAddSub is called when production ValueAddSub is exited.
func (s *BaseEQLListener) ExitValueAddSub(ctx *ValueAddSubContext) {}

// EnterValueNull is called when production ValueNull is entered.
func (s *BaseEQLListener) EnterValueNull(ctx *ValueNullContext) {}

// ExitValueNull is called when production ValueNull is exited.
func (s *BaseEQLListener) ExitValueNull(ctx *ValueNullContext) {}

// EnterValueMulDiv is called when production ValueMulDiv is entered.
func (s *BaseEQLListener) EnterValueMulDiv(ctx *ValueMulDivContext) {}

// ExitValueMulDiv is called when production ValueMulDiv is exited.
func (s *BaseEQLListener) ExitValueMulDiv(ctx *ValueMulDivContext) {}

// EnterValueGroup is called when production ValueGroup is entered.
func (s *BaseEQLListener) EnterValueGroup(ctx *ValueGroupContext) {}

// ExitValueGroup is called when production ValueGroup is exited.
func (s *BaseEQLListener) ExitValueGroup(ctx *ValueGroupContext) {}

// EnterValueLiteral is called when production ValueLiteral is entered.
func (s *BaseEQLListener) EnterValueLiteral(ctx *ValueLiteralContext) {}

// ExitValueLiteral is called when production ValueLiteral is exited.
func (s *BaseEQLListener) ExitValueLiteral(ctx *ValueLiteralContext) {}

// EnterValueFuncall is called when production ValueFuncall is entered.
func (s *BaseEQLListener) EnterValueFuncall(ctx *ValueFuncallContext) {}

// ExitValueFuncall is called when production ValueFuncall is exited.
func (s *BaseEQLListener) ExitValueFuncall(ctx *ValueFuncallContext) {}

// EnterValueField is called when production ValueField is entered.
func (s *BaseEQLListener) EnterValueField(ctx *ValueFieldContext) {}

// ExitValueField is called when production ValueField is exited.
func (s *BaseEQLListener) ExitValueField(ctx *ValueFieldContext) {}

// EnterPipeHead is called when production PipeHead is entered.
func (s *BaseEQLListener) EnterPipeHead(ctx *PipeHeadContext) {}

// ExitPipeHead is called when production PipeHead is exited.
func (s *BaseEQLListener) ExitPipeHead(ctx *PipeHeadContext) {}

// EnterPipeTail is called when production PipeTail is entered.
func (s *BaseEQLListener) EnterPipeTail(ctx *PipeTailContext) {}

// ExitPipeTail is called when production PipeTail is exited.
func (s *BaseEQLListener) ExitPipeTail(ctx *PipeTailContext) {}

// EnterPipeCount is called when production PipeCount is entered.
func (s *BaseEQLListener) EnterPipeCount(ctx *PipeCountContext) {}

// ExitPipeCount is called when production PipeCount is exited.
func (s *BaseEQLListener) ExitPipeCount(ctx *PipeCountContext) {}

// EnterPipeUnique is called when production PipeUnique is entered.
func (s *BaseEQLListener) EnterPipeUnique(ctx *PipeUniqueContext) {}

// ExitPipeUnique is called when production PipeUnique is exited.
func (s *BaseEQLListener) ExitPipeUnique(ctx *PipeUniqueContext) {}

// EnterPipeFilter is called when production PipeFilter is entered.
func (s *BaseEQLListener) EnterPipeFilter(ctx *PipeFilterContext) {}

// ExitPipeFilter is called when production PipeFilter is exited.
func (s *BaseEQLListener) ExitPipeFilter(ctx *PipeFilterContext) {}

// EnterPipeSort is called when production PipeSort is entered.
func (s *BaseEQLListener) EnterPipeSort(ctx *PipeSortContext) {}

// ExitPipeSort is called when production PipeSort is exited.
func (s *BaseEQLListener) ExitPipeSort(ctx *PipeSortContext) {}

// EnterFuncall is called when production funcall is entered.
func (s *BaseEQLListener) EnterFuncall(ctx *FuncallContext) {}

// ExitFuncall is called when production funcall is exited.
func (s *BaseEQLListener) ExitFuncall(ctx *FuncallContext) {}

// EnterFuncName is called when production funcName is entered.
func (s *BaseEQLListener) EnterFuncName(ctx *FuncNameContext) {}

// ExitFuncName is called when production funcName is exited.
func (s *BaseEQLListener) ExitFuncName(ctx *FuncNameContext) {}

// EnterInterval is called when production interval is entered.
func (s *BaseEQLListener) EnterInterval(ctx *IntervalContext) {}

// ExitInterval is called when production interval is exited.
func (s *BaseEQLListener) ExitInterval(ctx *IntervalContext) {}
