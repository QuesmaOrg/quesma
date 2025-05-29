// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"strings"
)

type materializedViewReplaceRule struct {
	tableName        string // table name that we want to replace
	condition        string // this is string representation of the condition that we want to replace
	materializedView string // target
}

type materializedViewReplace struct {
}

// it checks if the WHERE clause is `AND` tree only
func (s *materializedViewReplace) validateWhere(expr model.Expr) bool {

	var foundOR bool
	var foundNOT bool

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitPrefixExpr = func(b *model.BaseExprVisitor, e model.PrefixExpr) interface{} {

		if strings.ToUpper(e.Op) == "NOT" {
			foundNOT = true
			return e
		}

		b.VisitChildren(e.Args)
		return e
	}

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		if strings.ToUpper(e.Op) == "OR" {
			foundOR = true
			return e
		}
		e.Left.Accept(b)
		e.Right.Accept(b)
		return e
	}

	expr.Accept(visitor)

	if foundNOT {
		return false
	}

	if foundOR {
		return false
	}

	return true
}

func (s *materializedViewReplace) getTableName(tableName string) string {

	res := strings.Replace(tableName, `"`, "", -1)
	if strings.Contains(res, ".") {
		parts := strings.Split(res, ".")
		if len(parts) == 2 {
			return parts[1]
		}
	}
	return res
}

func (s *materializedViewReplace) matches(rule materializedViewReplaceRule, expr model.Expr) bool {
	current := model.AsString(expr)
	return rule.condition == current
}

func (s *materializedViewReplace) applyRule(rule materializedViewReplaceRule, expr model.Expr) (model.Expr, bool) {
	if s.matches(rule, expr) {
		return model.NewLiteral("TRUE"), true
	}

	return expr, false
}

func (s *materializedViewReplace) traverse(rule materializedViewReplaceRule, where model.Expr) (model.Expr, bool) {

	var foundInNot bool
	var replaced bool
	var res model.Expr

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {

		// since we replace with "TRUE" we need to check if the operator is "AND"
		if strings.ToUpper(e.Op) == "AND" {

			left, leftReplaced := s.applyRule(rule, e.Left)
			right, rightReplaced := s.applyRule(rule, e.Right)

			if !leftReplaced {
				left, leftReplaced = e.Left.Accept(b).(model.Expr)
			}

			if !rightReplaced {
				right, rightReplaced = e.Right.Accept(b).(model.Expr)
			}

			if leftReplaced || rightReplaced {
				replaced = true
			}
			return model.NewInfixExpr(left, e.Op, right)
		}

		return model.NewInfixExpr(e.Left.Accept(b).(model.Expr), e.Op, e.Right.Accept(b).(model.Expr))
	}

	res = where.Accept(visitor).(model.Expr)

	if foundInNot {
		return nil, false
	}

	return res, replaced
}

func (s *materializedViewReplace) replace(rule materializedViewReplaceRule, query model.SelectCommand) (*model.SelectCommand, bool) {

	visitor := model.NewBaseVisitor()
	var replaced bool

	visitor.OverrideVisitSelectCommand = func(v *model.BaseExprVisitor, query model.SelectCommand) interface{} {

		var namedCTEs []*model.CTE
		if query.NamedCTEs != nil {
			for _, cte := range query.NamedCTEs {
				namedCTEs = append(namedCTEs, cte.Accept(v).(*model.CTE))
			}
		}

		from := query.FromClause

		if from != nil {
			if table, ok := from.(model.TableRef); ok {

				tableName := s.getTableName(table.Name) // todo: get table name from data

				// if we match the table name
				if rule.tableName == tableName { // config param

					// we try to replace the where clause
					newWhere, whereReplaced := s.applyRule(rule, query.WhereClause)

					if !whereReplaced {
						// if we have AND tree, we try to traverse it
						if s.validateWhere(query.WhereClause) {
							// here we try to traverse the whole tree
							newWhere, whereReplaced = s.traverse(rule, query.WhereClause)
						}
					}

					// if we replaced the where clause, we replace the from clause
					if whereReplaced {
						replaced = true
						from = model.NewTableRef(rule.materializedView) // config param
						return model.NewSelectCommand(query.Columns, query.GroupBy, query.OrderBy, from, newWhere, query.LimitBy, query.Limit, query.SampleLimit, query.IsDistinct, namedCTEs)
					}
				}
			} else {
				from = query.FromClause.Accept(v).(model.Expr)
			}
		}
		where := query.WhereClause
		if query.WhereClause != nil {
			where = query.WhereClause.Accept(v).(model.Expr)
		}
		return model.NewSelectCommand(query.Columns, query.GroupBy, query.OrderBy, from, where, query.LimitBy, query.Limit, query.SampleLimit, query.IsDistinct, namedCTEs)

	}

	newSelect := query.Accept(visitor).(*model.SelectCommand)

	return newSelect, replaced
}

func (s *materializedViewReplace) readRule(properties map[string]string) materializedViewReplaceRule {
	rule := materializedViewReplaceRule{
		tableName:        properties["table"],
		condition:        properties["condition"],
		materializedView: properties["view"],
	}
	return rule
}

func (s *materializedViewReplace) Name() string {
	return "materialized_view_replace"
}

func (s *materializedViewReplace) IsEnabledByDefault() bool {
	return false
}

func (s *materializedViewReplace) Transform(plan *model.ExecutionPlan, properties map[string]string) (*model.ExecutionPlan, error) {

	//
	// TODO add list of rules maybe
	//
	rule := s.readRule(properties)

	for k, query := range plan.Queries {

		result, replaced := s.replace(rule, query.SelectCommand)

		// this is just in case if there was no truncation, we keep the original query
		if result != nil && replaced {
			logger.Info().Msgf(s.Name()+" triggered, input query: %s", query.SelectCommand.String())
			logger.Info().Msgf(s.Name()+" triggered, output query: %s", (*result).String())

			plan.Queries[k].SelectCommand = *result
			query.OptimizeHints.OptimizationsPerformed = append(query.OptimizeHints.OptimizationsPerformed, s.Name())
		}
	}
	return plan, nil
}
