package quesma

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/config"
	"strings"
)

type WhereVisitor struct {
	tableName string
	cfg       map[string]config.IndexConfiguration
}

func (v *WhereVisitor) VisitLiteral(e model.LiteralExpr) interface{} {
	return model.NewLiteral(e.Value)
}

func (v *WhereVisitor) VisitInfix(e model.InfixExpr) interface{} {
	const isIPAddressInRangePrimitive = "isIPAddressInRange"
	const CASTPrimitive = "CAST"
	const StringLiteral = "'String'"
	var lhs, rhs interface{}
	lhsValue := ""
	rhsValue := ""
	opValue := ""
	if e.Left != nil {
		lhs = e.Left.Accept(v)
		if lhs != nil {
			if lhsLiteral, ok := lhs.(model.LiteralExpr); ok {
				lhsValue = lhsLiteral.Value.(string)
			} else if lhsColumnRef, ok := lhs.(model.ColumnRef); ok {
				lhsValue = lhsColumnRef.ColumnName
			}
		}
	}
	if e.Right != nil {
		rhs = e.Right.Accept(v)
		if rhs != nil {
			if rhsLiteral, ok := rhs.(model.LiteralExpr); ok {
				rhsValue = rhsLiteral.Value.(string)
			} else if rhsColumnRef, ok := rhs.(model.ColumnRef); ok {
				rhsValue = rhsColumnRef.ColumnName
			}
		}
	}
	// skip transformation in the case of strict IP address
	if !strings.Contains(rhsValue, "/") {
		return model.NewInfixExpr(lhs.(model.Expr), e.Op, rhs.(model.Expr))
	}
	mappedType := v.cfg[v.tableName].TypeMappings[lhsValue]
	if mappedType != "ip" {
		return model.NewInfixExpr(lhs.(model.Expr), e.Op, rhs.(model.Expr))
	}
	if len(lhsValue) == 0 || len(rhsValue) == 0 {
		return model.NewInfixExpr(lhs.(model.Expr), e.Op, rhs.(model.Expr))
	}
	opValue = e.Op
	if opValue != "=" && opValue != "iLIKE" {
		logger.Warn().Msgf("ip transformation omitted, operator is not = or iLIKE: %s, lhs: %s, rhs: %s", opValue, lhsValue, rhsValue)
		return model.NewInfixExpr(lhs.(model.Expr), e.Op, rhs.(model.Expr))
	}
	rhsValue = strings.Replace(rhsValue, "%", "", -1)
	transformedWhereClause := &model.FunctionExpr{
		Name: isIPAddressInRangePrimitive,
		Args: []model.Expr{
			&model.FunctionExpr{
				Name: CASTPrimitive,
				Args: []model.Expr{
					&model.LiteralExpr{Value: lhsValue},
					&model.LiteralExpr{Value: StringLiteral},
				},
			},
			&model.LiteralExpr{Value: rhsValue},
		},
	}
	return transformedWhereClause
}

func (v *WhereVisitor) VisitPrefixExpr(e model.PrefixExpr) interface{} {
	for _, arg := range e.Args {
		if arg != nil {
			arg.Accept(v)
		}
	}
	return model.NewPrefixExpr(e.Op, e.Args)
}

func (v *WhereVisitor) VisitFunction(e model.FunctionExpr) interface{} {
	for _, arg := range e.Args {
		if arg != nil {
			arg.Accept(v)
		}
	}
	return model.NewFunction(e.Name, e.Args...)
}

func (v *WhereVisitor) VisitColumnRef(e model.ColumnRef) interface{} {
	return model.NewColumnRef(e.ColumnName)
}

func (v *WhereVisitor) VisitNestedProperty(e model.NestedProperty) interface{} {
	ColumnRef := e.ColumnRef.Accept(v).(model.ColumnRef)
	Property := e.PropertyName.Accept(v).(model.LiteralExpr)
	return model.NewNestedProperty(ColumnRef, Property)
}

func (v *WhereVisitor) VisitArrayAccess(e model.ArrayAccess) interface{} {
	e.ColumnRef.Accept(v)
	e.Index.Accept(v)
	return model.NewArrayAccess(e.ColumnRef, e.Index)
}

// TODO this whole block is fake ... need to double chceck this
func (v *WhereVisitor) MultiFunctionExpr(e model.MultiFunctionExpr) interface{}  { return e }
func (v *WhereVisitor) VisitMultiFunction(e model.MultiFunctionExpr) interface{} { return e }
func (v *WhereVisitor) VisitString(e model.StringExpr) interface{}               { return e }
func (v *WhereVisitor) VisitSQL(e model.SQL) interface{}                         { return e }
func (v *WhereVisitor) VisitOrderByExpr(e model.OrderByExpr) interface{}         { return e }
func (v *WhereVisitor) VisitDistinctExpr(e model.DistinctExpr) interface{}       { return e }
func (v *WhereVisitor) VisitTableRef(e model.TableRef) interface{}               { return e }

type SchemaCheckPass struct {
	cfg map[string]config.IndexConfiguration
}

// This functions trims the db name from the table name if exists
// We need to do this due to the way we are storing the schema in the config
// TableMap is indexed by table name, not db.table name
func getFromTable(fromTable string) string {
	// cut db name from table name if exists
	if idx := strings.IndexByte(fromTable, '.'); idx >= 0 {
		fromTable = fromTable[idx:]
		fromTable = strings.Trim(fromTable, ".")
	}
	return strings.Trim(fromTable, "\"")
}

// Below function applies schema transformations to the query regarding ip addresses.
// Internally, it converts sql statement like
// SELECT * FROM "kibana_sample_data_logs" WHERE lhs op rhs
// where op is '=' or 'iLIKE'
// into
// SELECT * FROM "kibana_sample_data_logs" WHERE isIPAddressInRange(CAST(lhs,'String'),rhs)
func (s *SchemaCheckPass) applyIpTransformations(query *model.Query) (*model.Query, error) {
	if query.WhereClause == nil {
		return query, nil
	}
	fromTable := getFromTable(query.TableName)
	whereVisitor := &WhereVisitor{tableName: fromTable, cfg: s.cfg}

	transformedWhereClause := query.WhereClause.Accept(whereVisitor)

	query.WhereClause = transformedWhereClause.(model.Expr)

	return query, nil
}

func (s *SchemaCheckPass) Transform(queries []*model.Query) ([]*model.Query, error) {
	for k, query := range queries {
		var err error
		inputQuery := query.String(context.Background())
		query, err = s.applyIpTransformations(query)
		if query.String(context.Background()) != inputQuery {
			logger.Info().Msgf("IpTransformation triggered, input query: %s", inputQuery)
			logger.Info().Msgf("IpTransformation triggered, output query: %s", query.String(context.Background()))
		}
		if err != nil {
			return nil, err
		}
		queries[k] = query
	}
	return queries, nil
}
