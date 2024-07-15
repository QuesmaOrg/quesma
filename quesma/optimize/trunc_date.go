// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"fmt"
	"quesma/model"
	"strings"
	"time"
)

// truncateDateVisitor - a visitor that truncates dates in the query
// It finds date comparisons like:
//
// column >= '2024-06-04T13:08:53.675Z' and column <= '2024-06-06T13:10:53.675Z'
//
// and truncates the dates to the nearest 5 minutes (or any other duration), resulting in:
//
// column >= '2024-06-04T13:05:00.000Z' and column <= '2024-06-06T13:15:00.000Z'
//
// Note: Truncation is done only if the difference between the dates is more than 24 hours.

type truncateDateVisitor struct {
	truncateTo time.Duration
	truncated  bool
}

func (v *truncateDateVisitor) visitChildren(args []model.Expr) []model.Expr {
	var newArgs []model.Expr
	for _, arg := range args {
		if arg != nil {
			newArgs = append(newArgs, arg.Accept(v).(model.Expr))
		}
	}
	return newArgs
}

func (v *truncateDateVisitor) processDateNode(e model.Expr) (string, string, bool) {

	if fn, ok := e.(model.FunctionExpr); ok {

		if fn.Name == "parseDateTime64BestEffort" {

			if len(fn.Args) == 1 {

				//"2024-06-04T13:08:53.675Z" -> "2024-06-04T13:08:00.000Z"

				if date, ok := fn.Args[0].(model.LiteralExpr); ok {
					if dateStr, ok := date.Value.(string); ok {

						dateStr = strings.Trim(dateStr, "'")
						return fn.Name, dateStr, true
					}
				}
			}
		}
	}
	return "", "", false
}

type dateComparisonNode struct {
	column string
	op     string
	fn     string
	date   string
}

func (v *truncateDateVisitor) processDateComparisonNode(e model.Expr) *dateComparisonNode {

	if op, ok := e.(model.InfixExpr); ok && (op.Op == ">" || op.Op == "<" || op.Op == ">=" || op.Op == "<=") {
		if column, ok := op.Left.(model.ColumnRef); ok {
			if fn, date, ok := v.processDateNode(op.Right); ok {
				return &dateComparisonNode{column: column.ColumnName, op: op.Op, fn: fn, date: date}
			}
		}
	}
	return nil
}

func (v *truncateDateVisitor) compare(column, op string, right model.Expr) model.Expr {
	return model.NewInfixExpr(model.NewColumnRef(column), op, right)
}

func (v *truncateDateVisitor) date(fn, str string) model.Expr {
	return model.NewFunction(fn, model.NewLiteral(fmt.Sprintf("'%s'", str)))
}

func (v *truncateDateVisitor) truncateDate(op string, t time.Time) string {

	truncatedDate := t.Truncate(v.truncateTo)

	if op == "<" || op == "<=" {
		truncatedDate = truncatedDate.Add(v.truncateTo)
	}

	return truncatedDate.Format(time.RFC3339)
}

func (v *truncateDateVisitor) VisitLiteral(e model.LiteralExpr) interface{} {
	return e
}

// truncate - truncates the date if the difference between the dates is more than 24 hours
// returns nil if the truncation is not possible or not needed
func (v *truncateDateVisitor) truncate(e model.InfixExpr) interface{} {

	left := v.processDateComparisonNode(e.Left)
	right := v.processDateComparisonNode(e.Right)

	if left != nil && right != nil {

		// check if the columns are the same,
		if left.column == right.column {

			if leftTime, err := time.Parse(time.RFC3339, left.date); err == nil {
				if rightTime, err := time.Parse(time.RFC3339, right.date); err == nil {

					duration := rightTime.Sub(leftTime).Abs()

					// if the duration is more than 24 hours, we can truncate the date
					if duration > 24*time.Hour {

						newLeft := v.truncateDate(left.op, leftTime)
						newRight := v.truncateDate(right.op, rightTime)

						v.truncated = true

						res := model.NewInfixExpr(
							v.compare(left.column, left.op, v.date(left.fn, newLeft)),
							e.Op,
							v.compare(right.column, right.op, v.date(right.fn, newRight)))

						return res
					}
				}
			}
		}
	}

	return nil
}

func (v *truncateDateVisitor) VisitInfix(e model.InfixExpr) interface{} {

	op := strings.ToLower(e.Op)
	if op == "and" {
		truncatedExpr := v.truncate(e)

		if truncatedExpr != nil {
			return truncatedExpr
		}
	}

	// no truncation
	left := e.Left.Accept(v).(model.Expr)
	right := e.Right.Accept(v).(model.Expr)
	return model.NewInfixExpr(left, e.Op, right)
}

func (v *truncateDateVisitor) VisitPrefixExpr(e model.PrefixExpr) interface{} {
	args := v.visitChildren(e.Args)
	return model.NewPrefixExpr(e.Op, args)

}

func (v *truncateDateVisitor) VisitFunction(e model.FunctionExpr) interface{} {
	args := v.visitChildren(e.Args)
	return model.NewFunction(e.Name, args...)
}

func (v *truncateDateVisitor) VisitColumnRef(e model.ColumnRef) interface{} {
	return e
}

func (v *truncateDateVisitor) VisitNestedProperty(e model.NestedProperty) interface{} {
	return model.NestedProperty{
		ColumnRef:    e.ColumnRef.Accept(v).(model.ColumnRef),
		PropertyName: e.PropertyName.Accept(v).(model.LiteralExpr),
	}
}

func (v *truncateDateVisitor) VisitArrayAccess(e model.ArrayAccess) interface{} {
	return model.ArrayAccess{
		ColumnRef: e.ColumnRef.Accept(v).(model.ColumnRef),
		Index:     e.Index.Accept(v).(model.Expr),
	}
}

func (v *truncateDateVisitor) VisitMultiFunction(e model.MultiFunctionExpr) interface{} {
	args := v.visitChildren(e.Args)
	return model.MultiFunctionExpr{Name: e.Name, Args: args}
}

func (v *truncateDateVisitor) VisitString(e model.StringExpr) interface{} { return e }

func (v *truncateDateVisitor) VisitOrderByExpr(e model.OrderByExpr) interface{} {
	exprs := v.visitChildren(e.Exprs)
	return model.NewOrderByExpr(exprs, e.Direction)

}
func (v *truncateDateVisitor) VisitDistinctExpr(e model.DistinctExpr) interface{} {
	return model.NewDistinctExpr(e.Expr.Accept(v).(model.Expr))
}
func (v *truncateDateVisitor) VisitTableRef(e model.TableRef) interface{} {
	return model.NewTableRef(e.Name)
}
func (v *truncateDateVisitor) VisitAliasedExpr(e model.AliasedExpr) interface{} {
	return model.NewAliasedExpr(e.Expr.Accept(v).(model.Expr), e.Alias)
}
func (v *truncateDateVisitor) VisitWindowFunction(e model.WindowFunction) interface{} {
	return model.NewWindowFunction(e.Name, v.visitChildren(e.Args), v.visitChildren(e.PartitionBy), e.OrderBy.Accept(v).(model.OrderByExpr))
}

func (v *truncateDateVisitor) VisitSelectCommand(e model.SelectCommand) interface{} {

	// transformation

	var groupBy []model.Expr

	for _, expr := range e.GroupBy {
		groupBy = append(groupBy, expr.Accept(v).(model.Expr))
	}

	var columns []model.Expr
	for _, expr := range e.Columns {
		columns = append(columns, expr.Accept(v).(model.Expr))
	}

	var fromClause model.Expr
	if e.FromClause != nil {
		fromClause = e.FromClause.Accept(v).(model.Expr)
	}

	var whereClause model.Expr
	if e.WhereClause != nil {
		whereClause = e.WhereClause.Accept(v).(model.Expr)
	}

	var ctes []model.SelectCommand
	if e.CTEs != nil {
		ctes = make([]model.SelectCommand, 0)
		for _, cte := range e.CTEs {
			ctes = append(ctes, cte.Accept(v).(model.SelectCommand))
		}
	}

	return model.NewSelectCommand(columns, groupBy, e.OrderBy,
		fromClause, whereClause, e.LimitBy, e.Limit, e.SampleLimit, e.IsDistinct, ctes)

}

func (v *truncateDateVisitor) VisitParenExpr(p model.ParenExpr) interface{} {
	var exprs []model.Expr
	for _, expr := range p.Exprs {
		exprs = append(exprs, expr.Accept(v).(model.Expr))
	}
	return model.NewParenExpr(exprs...)
}

func (v *truncateDateVisitor) VisitLambdaExpr(e model.LambdaExpr) interface{} {
	return model.NewLambdaExpr(e.Args, e.Body.Accept(v).(model.Expr))
}

type truncateDate struct {
	truncateTo time.Duration
}

func (s *truncateDate) Transform(queries []*model.Query) ([]*model.Query, error) {

	for k, query := range queries {
		visitor := &truncateDateVisitor{}

		visitor.truncateTo = s.truncateTo

		result := query.SelectCommand.Accept(visitor).(*model.SelectCommand)

		// this is just in case if there was no truncation, we keep the original query
		if visitor.truncated && result != nil {
			queries[k].SelectCommand = *result
			query.OptimizeHints.OptimizationsPerformed = append(query.OptimizeHints.OptimizationsPerformed, "truncateDate")
		}
	}
	return queries, nil
}
