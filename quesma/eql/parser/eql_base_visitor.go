// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
// Code generated from quesma/eql/parser/EQL.g4 by ANTLR 4.13.1. DO NOT EDIT.

package parser // EQL
import "github.com/antlr4-go/antlr/v4"

type BaseEQLVisitor struct {
	*antlr.BaseParseTreeVisitor
}

func (v *BaseEQLVisitor) VisitQuery(ctx *QueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitSimpleQuery(ctx *SimpleQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitSequenceQuery(ctx *SequenceQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitSampleQuery(ctx *SampleQueryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitLookupOpList(ctx *LookupOpListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitComparisonOp(ctx *ComparisonOpContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitConditionNotFuncall(ctx *ConditionNotFuncallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitConditionBoolean(ctx *ConditionBooleanContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitConditionNot(ctx *ConditionNotContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitLookupNotOpList(ctx *LookupNotOpListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitConditionLogicalOp(ctx *ConditionLogicalOpContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitConditionGroup(ctx *ConditionGroupContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitConditionFuncall(ctx *ConditionFuncallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitCategory(ctx *CategoryContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitField(ctx *FieldContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitFieldList(ctx *FieldListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitLiteral(ctx *LiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitLiteralList(ctx *LiteralListContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitValueAddSub(ctx *ValueAddSubContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitValueNull(ctx *ValueNullContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitValueMulDiv(ctx *ValueMulDivContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitValueGroup(ctx *ValueGroupContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitValueLiteral(ctx *ValueLiteralContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitValueFuncall(ctx *ValueFuncallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitValueField(ctx *ValueFieldContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitPipeHead(ctx *PipeHeadContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitPipeTail(ctx *PipeTailContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitPipeCount(ctx *PipeCountContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitPipeUnique(ctx *PipeUniqueContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitPipeFilter(ctx *PipeFilterContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitPipeSort(ctx *PipeSortContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitFuncall(ctx *FuncallContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitFuncName(ctx *FuncNameContext) interface{} {
	return v.VisitChildren(ctx)
}

func (v *BaseEQLVisitor) VisitInterval(ctx *IntervalContext) interface{} {
	return v.VisitChildren(ctx)
}
