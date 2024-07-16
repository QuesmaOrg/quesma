// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package quesma

import (
	"quesma/model"
	"quesma/quesma/config"
)

type IndexMappingRewriter struct {
	sourceToDestMapping map[string]string
}

func (IndexMappingRewriter) VisitFunction(e model.FunctionExpr) interface{}           { return e }
func (IndexMappingRewriter) VisitMultiFunction(e model.MultiFunctionExpr) interface{} { return e }
func (IndexMappingRewriter) VisitLiteral(l model.LiteralExpr) interface{}             { return l }
func (IndexMappingRewriter) VisitString(e model.StringExpr) interface{}               { return e }
func (IndexMappingRewriter) VisitInfix(e model.InfixExpr) interface{}                 { return e }
func (IndexMappingRewriter) VisitColumnRef(e model.ColumnRef) interface{}             { return e }
func (IndexMappingRewriter) VisitPrefixExpr(e model.PrefixExpr) interface{}           { return e }
func (IndexMappingRewriter) VisitNestedProperty(e model.NestedProperty) interface{}   { return e }
func (IndexMappingRewriter) VisitArrayAccess(e model.ArrayAccess) interface{}         { return e }
func (IndexMappingRewriter) VisitOrderByExpr(e model.OrderByExpr) interface{}         { return e }
func (IndexMappingRewriter) VisitDistinctExpr(e model.DistinctExpr) interface{}       { return e }
func (IndexMappingRewriter) VisitTableRef(e model.TableRef) interface{} {
	return model.NewTableRef(e.Name)
}
func (v *IndexMappingRewriter) VisitAliasedExpr(e model.AliasedExpr) interface{} { return e }
func (v *IndexMappingRewriter) VisitSelectCommand(e model.SelectCommand) interface{} {
	fromClause := e.FromClause.Accept(v)
	return model.NewSelectCommand(e.Columns, e.GroupBy, e.OrderBy,
		fromClause.(model.Expr), e.WhereClause, e.LimitBy, e.Limit, e.SampleLimit, e.IsDistinct, e.CTEs)
}
func (IndexMappingRewriter) VisitWindowFunction(f model.WindowFunction) interface{} { return f }
func (IndexMappingRewriter) VisitParenExpr(e model.ParenExpr) interface{}           { return e }
func (IndexMappingRewriter) VisitLambdaExpr(e model.LambdaExpr) interface{}         { return e }

func NewIndexMappingRewriter(indexMappings map[string]config.IndexMappingsConfiguration) *IndexMappingRewriter {
	rewriter := &IndexMappingRewriter{sourceToDestMapping: make(map[string]string)}
	for _, indexMapping := range indexMappings {
		for _, sourceIndex := range indexMapping.Mappings {
			destIndex := indexMapping.Name
			rewriter.sourceToDestMapping[sourceIndex] = destIndex
		}
	}
	return rewriter
}

func (s *SchemaCheckPass) applyIndexMappingTransformations(query *model.Query) (*model.Query, error) {
	indexMappingRewriter := NewIndexMappingRewriter(s.indexMappings)
	expr := query.SelectCommand.Accept(indexMappingRewriter)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil

}
