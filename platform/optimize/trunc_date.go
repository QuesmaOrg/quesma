// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package optimize

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/model"
	"strings"
	"time"
)

// truncDateProcessor - a visitor that truncates dates in the query
// It finds date comparisons like:
//
// column >= '2024-06-04T13:08:53.675Z' and column <= '2024-06-06T13:10:53.675Z'
//
// and truncates the dates to the nearest 5 minutes (or any other duration), resulting in:
//
// column >= '2024-06-04T13:05:00.000Z' and column <= '2024-06-06T13:15:00.000Z'
//
// Note: Truncation is done only if the difference between the dates is more than 24 hours.

type truncDateProcessor struct {
	truncateTo time.Duration
	truncated  bool
}

func (v *truncDateProcessor) processDateNode(e model.Expr) (string, string, bool) {

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

func (v *truncDateProcessor) processDateComparisonNode(e model.Expr) *dateComparisonNode {

	if op, ok := e.(model.InfixExpr); ok && (op.Op == ">" || op.Op == "<" || op.Op == ">=" || op.Op == "<=") {
		if column, ok := op.Left.(model.ColumnRef); ok {
			if fn, date, ok := v.processDateNode(op.Right); ok {
				return &dateComparisonNode{column: column.ColumnName, op: op.Op, fn: fn, date: date}
			}
		}
	}
	return nil
}

func (v *truncDateProcessor) compare(column, op string, right model.Expr) model.Expr {
	return model.NewInfixExpr(model.NewColumnRef(column), op, right)
}

func (v *truncDateProcessor) date(fn, str string) model.Expr {
	return model.NewFunction(fn, model.NewLiteral(fmt.Sprintf("'%s'", str)))
}

func (v *truncDateProcessor) truncateDate(op string, t time.Time) string {

	truncatedDate := t.Truncate(v.truncateTo)

	if op == "<" || op == "<=" {
		truncatedDate = truncatedDate.Add(v.truncateTo)
	}

	return truncatedDate.Format(time.RFC3339)
}

// truncate - truncates the date if the difference between the dates is more than 24 hours
// returns nil if the truncation is not possible or not needed
func (v *truncDateProcessor) truncate(e model.InfixExpr) interface{} {

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

func newTruncDate(truncateTo time.Duration) (model.ExprVisitor, *truncDateProcessor) {

	v := &truncDateProcessor{truncateTo: truncateTo}

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		op := strings.ToLower(e.Op)
		if op == "and" {
			truncatedExpr := v.truncate(e)

			if truncatedExpr != nil {
				return truncatedExpr
			}
		}

		// no truncation
		left := e.Left.Accept(b).(model.Expr)
		right := e.Right.Accept(b).(model.Expr)
		return model.NewInfixExpr(left, e.Op, right)
	}

	return visitor, v
}

type truncateDate struct {
	truncateTo time.Duration
}

func (s *truncateDate) Name() string {
	return "truncate_date"
}

func (s *truncateDate) IsEnabledByDefault() bool {
	// This optimization is not enabled by default.
	// Tt returns different results than the original query
	// So it should be used with caution
	return false
}

func (s *truncateDate) Transform(queries []*model.Query, properties map[string]string) ([]*model.Query, error) {

	for k, query := range queries {

		visitor, processor := newTruncDate(s.truncateTo) // read from properties

		result := query.SelectCommand.Accept(visitor).(*model.SelectCommand)

		// this is just in case if there was no truncation, we keep the original query
		if processor.truncated && result != nil {
			queries[k].SelectCommand = *result
			query.OptimizeHints.OptimizationsPerformed = append(query.OptimizeHints.OptimizationsPerformed, s.Name())
		}
	}
	return queries, nil
}
