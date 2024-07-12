package optimize

import (
	"fmt"
	"quesma/logger"
	"quesma/model"
	"strings"
	"time"
)

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

func (v *truncateDateVisitor) VisitLiteral(e model.LiteralExpr) interface{} {

	switch v := e.Value.(type) {

	case string:
		// "2024-06-04T13:08:53.675Z"
		//
		if strings.HasPrefix(v, "2024-") {
			logger.Warn().Msgf("not handled date to truncate: '%s'. This can be a bug.", v)
		}

	default:
		return e
	}

	return e
}

func (v *truncateDateVisitor) VisitInfix(e model.InfixExpr) interface{} {
	left := e.Left.Accept(v).(model.Expr)
	right := e.Right.Accept(v).(model.Expr)

	return model.NewInfixExpr(left, e.Op, right)
}

func (v *truncateDateVisitor) VisitPrefixExpr(e model.PrefixExpr) interface{} {
	args := v.visitChildren(e.Args)
	return model.NewPrefixExpr(e.Op, args)

}

func (v *truncateDateVisitor) VisitFunction(e model.FunctionExpr) interface{} {

	// TODO what other functions should we handle?
	if e.Name == "parseDateTime64BestEffort" {

		if len(e.Args) == 1 {

			//"2024-06-04T13:08:53.675Z" -> "2024-06-04T13:08:00.000Z"

			if date, ok := e.Args[0].(model.LiteralExpr); ok {
				if dateStr, ok := date.Value.(string); ok {

					dateStr = strings.Trim(dateStr, "'")

					// Parse the date string into a time.Time object
					d, err := time.Parse(time.RFC3339, dateStr)
					if err != nil {
						// can parse the date, return the original expression
					} else {
						truncatedDate := d.Truncate(v.truncateTo)
						truncatedDateStr := truncatedDate.Format(time.RFC3339)
						v.truncated = true

						fmt.Println("DATE TRUNCATED: ", d, " -> ", truncatedDateStr)
						return model.NewFunction(e.Name, model.NewLiteral(fmt.Sprintf("'%s'", truncatedDateStr)))
					}
				}
			}
		}
	}

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

	return model.NewSelectCommand(columns, groupBy, e.OrderBy,
		fromClause, whereClause, e.Limit, e.SampleLimit, e.IsDistinct)

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
