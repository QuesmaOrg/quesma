package model

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
)

type AliasResolver struct {
	Cfg map[string]config.IndexConfiguration
}

func (a *AliasResolver) Transform(queries []*Query) ([]*Query, error) {
	for i, query := range queries {
		logger.Info().Msgf("PRZEMYSLAW APPLIES ALIASES")
		aliases := a.aliasesConfigured(query.TableName)
		if len(aliases) == 0 {
			queries[i] = query
		} else {
			queries[i].SelectCommand = a.applyAliases(*queries[i], aliases).(SelectCommand)
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
func (a *AliasResolver) resolveAlias(columnName string, aliases []config.FieldAlias) string {
	for _, aliasCfg := range aliases {
		if aliasCfg.SourceFieldName == columnName {
			return aliasCfg.TargetFieldName
		}
	}
	return ""
}

func (a *AliasResolver) applyAliases(query Query, aliases []config.FieldAlias) Expr {
	ctx := context.WithValue(context.Background(), "aliases", aliases)
	return query.SelectCommand.Accept(ctx, a).(Expr)
}

func (a *AliasResolver) VisitPrefixExpr(ctx context.Context, e PrefixExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Args {
		exprs = append(exprs, expr.Accept(ctx, a).(Expr))
	}
	return NewPrefixExpr(e.Op, exprs)
}

func (a *AliasResolver) VisitNestedProperty(ctx context.Context, e NestedProperty) interface{} {
	return NewNestedProperty(e.ColumnRef.Accept(ctx, a).(ColumnRef), e.PropertyName)
}

func (a *AliasResolver) VisitArrayAccess(ctx context.Context, e ArrayAccess) interface{} {
	return NewArrayAccess(e.ColumnRef.Accept(ctx, a).(ColumnRef), e.Index.Accept(ctx, a).(Expr))
}

func (a *AliasResolver) VisitColumnRef(ctx context.Context, e ColumnRef) interface{} {
	var aliasesList []config.FieldAlias
	if v, ok := ctx.Value("aliases").([]config.FieldAlias); !ok {
		logger.Error().Msg("Aliases not found in context")
		return e
	} else {
		aliasesList = v
	}

	if newColumnName := a.resolveAlias(e.ColumnName, aliasesList); newColumnName != "" {
		logger.Info().Msgf("[PRZEMYSLAW] SWAPS [%s] with [%s]", e.ColumnName, newColumnName)
		return NewColumnRef(newColumnName)
	}
	return e
}

func (a *AliasResolver) VisitFunction(ctx context.Context, e FunctionExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Args {
		exprs = append(exprs, expr.Accept(ctx, a).(Expr))
	}
	return NewFunction(e.Name, exprs...)
}

func (a *AliasResolver) VisitLiteral(_ context.Context, l LiteralExpr) interface{} {
	return NewLiteral(l.Value)
}

func (a *AliasResolver) VisitMultiFunction(ctx context.Context, f MultiFunctionExpr) interface{} {
	var exprs []Expr
	for _, expr := range f.Args {
		exprs = append(exprs, expr.Accept(ctx, a).(Expr))
	}
	return MultiFunctionExpr{Name: f.Name, Args: exprs}
}

func (a *AliasResolver) VisitInfix(ctx context.Context, e InfixExpr) interface{} {
	return NewInfixExpr(e.Left.Accept(ctx, a).(Expr), e.Op, e.Right.Accept(ctx, a).(Expr))
}

func (a *AliasResolver) VisitString(_ context.Context, e StringExpr) interface{} {
	return NewStringExpr(e.Value)
}

func (a *AliasResolver) VisitOrderByExpr(ctx context.Context, e OrderByExpr) interface{} {
	var exprs []Expr
	for _, expr := range e.Exprs {
		exprs = append(exprs, expr.Accept(ctx, a).(Expr))
	}
	return NewOrderByExpr(exprs, e.Direction)
}

func (a *AliasResolver) VisitDistinctExpr(ctx context.Context, e DistinctExpr) interface{} {
	return NewDistinctExpr(e.Expr.Accept(ctx, a).(Expr))
}

func (a *AliasResolver) VisitTableRef(ctx context.Context, e TableRef) interface{} {
	return NewTableRef(e.Name)
}

func (a *AliasResolver) VisitAliasedExpr(ctx context.Context, e AliasedExpr) interface{} {
	return NewAliasedExpr(e.Expr.Accept(ctx, a).(Expr), e.Alias)
}
func (a *AliasResolver) VisitSelectCommand(ctx context.Context, s SelectCommand) interface{} {
	var columns, groupBy []Expr
	var orderBy []OrderByExpr
	for _, expr := range s.Columns {
		columns = append(columns, expr.Accept(ctx, a).(Expr))
	}
	for _, expr := range s.GroupBy {
		groupBy = append(groupBy, expr.Accept(ctx, a).(Expr))
	}
	for _, expr := range s.OrderBy {
		orderBy = append(orderBy, expr.Accept(ctx, a).(OrderByExpr))
	}
	return *NewSelectCommand(columns, groupBy, orderBy, s.FromClause.Accept(ctx, a).(Expr), s.WhereClause.Accept(ctx, a).(Expr), s.Limit, s.SampleLimit, s.IsDistinct)

}

func (a *AliasResolver) VisitWindowFunction(ctx context.Context, f WindowFunction) interface{} {
	var args, partitionBy []Expr
	for _, expr := range f.Args {
		args = append(args, expr.Accept(ctx, a).(Expr))
	}
	for _, expr := range f.PartitionBy {
		partitionBy = append(partitionBy, expr.Accept(ctx, a).(Expr))
	}
	return NewWindowFunction(f.Name, args, partitionBy, f.OrderBy.Accept(ctx, a).(OrderByExpr))
}
