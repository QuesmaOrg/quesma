package model

import (
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
)

type AliasResolver struct {
	Cfg map[string]config.IndexConfiguration
	//aliases []config.FieldAlias
}

func (a *AliasResolver) Transform(queries []*Query) ([]*Query, error) {
	for i, query := range queries {
		logger.Info().Msgf("PRZEMYSLAW APPLIES ALIASES")
		if len(a.aliasesConfigured(query.TableName)) == 0 {
			queries[i] = query
		} else {
			queries[i].SelectCommand = a.applyAliases(*queries[i]).(SelectCommand)
		}
	}
	return queries, nil
}

// aliasesConfigured returns a list of aliases for a given table name, WARNING: it's using old configuration layout
func (a *AliasResolver) aliasesConfigured(tableName string) (aliasesList []config.FieldAlias) {
	if aliases, ok := a.Cfg[tableName]; !ok {
		return
	} else {
		for _, alias := range aliases.Aliases {
			aliasesList = append(aliasesList, alias)
		}
		return
	}
}

// resolveAlias returns a list of aliases for a given table name, WARNING: it's using old configuration layout
func (a *AliasResolver) resolveAlias(columnName string) string {
	for _, aliasCfg := range aliases {
		if aliasCfg.SourceFieldName == columnName {
			return aliasCfg.TargetFieldName
		}
	}
	return ""
}

func (a *AliasResolver) applyAliases(query Query) Expr {
	return query.SelectCommand.Accept(a).(Expr)
}

func (a *AliasResolver) VisitPrefixExpr(e PrefixExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Args {
		exprs = append(exprs, expr.Accept(a).(Expr))
	}
	return NewPrefixExpr(e.Op, exprs)
}

func (a *AliasResolver) VisitNestedProperty(e NestedProperty) interface{} {
	return NewNestedProperty(e.ColumnRef.Accept(a).(ColumnRef), e.PropertyName)
}

func (a *AliasResolver) VisitArrayAccess(e ArrayAccess) interface{} {
	return NewArrayAccess(e.ColumnRef.Accept(a).(ColumnRef), e.Index.Accept(a).(Expr))
}

func (a *AliasResolver) VisitColumnRef(e ColumnRef) interface{} {
	if newColumnName := a.resolveAlias(e.ColumnName); newColumnName != "" {
		logger.Info().Msgf("[PRZEMYSLAW] SWAPS [%s] with [%s]", e.ColumnName, newColumnName)
		return NewColumnRef(newColumnName)
	}
	return e
}

func (a *AliasResolver) VisitFunction(e FunctionExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Args {
		exprs = append(exprs, expr.Accept(a).(Expr))
	}
	return NewFunction(e.Name, exprs...)
}

func (a *AliasResolver) VisitLiteral(l LiteralExpr) interface{} {
	return NewLiteral(l.Value)
}

func (a *AliasResolver) VisitMultiFunction(f MultiFunctionExpr) interface{} {
	var exprs []Expr
	for _, expr := range f.Args {
		exprs = append(exprs, expr.Accept(a).(Expr))
	}
	return MultiFunctionExpr{Name: f.Name, Args: exprs}
}

func (a *AliasResolver) VisitInfix(e InfixExpr) interface{} {
	return NewInfixExpr(e.Left.Accept(a).(Expr), e.Op, e.Right.Accept(a).(Expr))
}

func (a *AliasResolver) VisitString(e StringExpr) interface{} {
	return NewStringExpr(e.Value)
}

func (a *AliasResolver) VisitOrderByExpr(e OrderByExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Exprs {
		exprs = append(exprs, expr.Accept(a).(Expr))
	}
	return NewOrderByExpr(exprs, e.Direction)
}

func (a *AliasResolver) VisitDistinctExpr(e DistinctExpr) interface{} {
	return NewDistinctExpr(e.Expr.Accept(a).(Expr))
}

func (a *AliasResolver) VisitTableRef(e TableRef) interface{} {
	return NewTableRef(e.Name)
}

func (a *AliasResolver) VisitAliasedExpr(e AliasedExpr) interface{} {
	return NewAliasedExpr(e.Expr.Accept(a).(Expr), e.Alias)
}
func (a *AliasResolver) VisitSelectCommand(s SelectCommand) interface{} {
	var columns, groupBy []Expr
	var orderBy []OrderByExpr
	for _, expr := range s.Columns {
		columns = append(columns, expr.Accept(a).(Expr))
	}
	for _, expr := range s.GroupBy {
		groupBy = append(groupBy, expr.Accept(a).(Expr))
	}
	for _, expr := range s.OrderBy {
		orderBy = append(orderBy, expr.Accept(a).(OrderByExpr))
	}
	return *NewSelectCommand(columns, groupBy, orderBy, s.FromClause.Accept(a).(Expr), s.WhereClause.Accept(a).(Expr), s.Limit, s.SampleLimit, s.IsDistinct)

}

func (a *AliasResolver) VisitWindowFunction(f WindowFunction) interface{} {
	var args, partitionBy []Expr
	for _, expr := range f.Args {
		args = append(args, expr.Accept(a).(Expr))
	}
	for _, expr := range f.PartitionBy {
		partitionBy = append(partitionBy, expr.Accept(a).(Expr))
	}
	return NewWindowFunction(f.Name, args, partitionBy, f.OrderBy.Accept(a).(OrderByExpr))
}
