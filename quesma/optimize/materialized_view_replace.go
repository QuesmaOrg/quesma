// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"quesma/logger"
	"quesma/model"
	"strings"
)

type materializedViewReplaceRule struct {
	tableName        string // table name that we want to replace
	condition        string // this is string representation of the condition that we want to replace
	materializedView string // target
}

type materializedViewReplace struct {
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

func (s *materializedViewReplace) replaceInfix(where model.Expr, pattern string, replacement model.Expr) (model.Expr, bool) {

	var replaced bool
	var res model.Expr

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {

		current := model.AsString(e)
		if pattern == current {
			replaced = true
			return replacement
		}
		return model.NewInfixExpr(e.Left.Accept(b).(model.Expr), e.Op, e.Right.Accept(b).(model.Expr))
	}

	res = where.Accept(visitor).(model.Expr)

	return res, replaced
}

func (s *materializedViewReplace) replace(rule materializedViewReplaceRule, query model.SelectCommand) (*model.SelectCommand, bool) {

	visitor := model.NewBaseVisitor()
	var replaced bool

	visitor.OverrideVisitSelectCommand = func(v *model.BaseExprVisitor, query model.SelectCommand) interface{} {

		var ctes []*model.SelectCommand
		if query.CTEs != nil {
			ctes = make([]*model.SelectCommand, 0)
			for _, cte := range query.CTEs {
				ctes = append(ctes, cte.Accept(v).(*model.SelectCommand))
			}
		}

		from := query.FromClause

		if from != nil {
			if table, ok := from.(model.TableRef); ok {

				tableName := s.getTableName(table.Name) // todo: get table name from data

				// if we match the table name
				if rule.tableName == tableName { // config param

					// we try to replace the where clause
					newWhere, whereReplaced := s.replaceInfix(query.WhereClause, rule.condition, model.NewLiteral("TRUE"))

					// if we replaced the where clause, we replace the from clause
					if whereReplaced {
						replaced = true
						from = model.NewTableRef(rule.materializedView) // config param
						return model.NewSelectCommand(query.Columns, query.GroupBy, query.OrderBy, from, newWhere, query.LimitBy, query.Limit, query.SampleLimit, query.IsDistinct, ctes)
					}
				}
			} else {
				from = query.FromClause.Accept(v).(model.Expr)
			}
		}

		where := query.WhereClause.Accept(v).(model.Expr)

		return model.NewSelectCommand(query.Columns, query.GroupBy, query.OrderBy, from, where, query.LimitBy, query.Limit, query.SampleLimit, query.IsDistinct, ctes)

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

func (s *materializedViewReplace) Transform(queries []*model.Query, properties map[string]string) ([]*model.Query, error) {

	//
	// TODO add list of rules maybe
	//
	rule := s.readRule(properties)

	for k, query := range queries {

		result, replaced := s.replace(rule, query.SelectCommand)

		// this is just in case if there was no truncation, we keep the original query
		if result != nil && replaced {
			logger.Info().Msgf(s.Name()+" triggered, input query: %s", query.SelectCommand.String())
			logger.Info().Msgf(s.Name()+" triggered, output query: %s", (*result).String())

			queries[k].SelectCommand = *result
			query.OptimizeHints.OptimizationsPerformed = append(query.OptimizeHints.OptimizationsPerformed, s.Name())
		}
	}
	return queries, nil
}
