package quesma

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/where_clause"
	"mitmproxy/quesma/quesma/config"
	"strings"
)

type WhereVisitor struct {
	lhs       string
	rhs       string
	op        string
	tableName string
	cfg       map[string]config.IndexConfiguration
}

func (v *WhereVisitor) VisitLiteral(e *where_clause.Literal) interface{} {
	return where_clause.NewLiteral(e.Name)
}

func (v *WhereVisitor) VisitInfixOp(e *where_clause.InfixOp) interface{} {
	const isIPAddressInRangePrimitive = "isIPAddressInRange"
	const CASTPrimitive = "CAST"
	const StringLiteral = "'String'"
	var lhs, rhs interface{}
	v.lhs = ""
	v.lhs = ""
	v.op = ""
	if e.Left != nil {
		lhs = e.Left.Accept(v)
		if lhs != nil {
			if lhsLiteral, ok := lhs.(*where_clause.Literal); ok {
				v.lhs = lhsLiteral.Name
			} else if lhsColumnRef, ok := lhs.(*where_clause.ColumnRef); ok {
				v.lhs = lhsColumnRef.ColumnName
			} else {
				v.lhs = ""
			}
		}
	}
	if e.Right != nil {
		rhs = e.Right.Accept(v)
		if rhs != nil {
			if rhsLiteral, ok := rhs.(*where_clause.Literal); ok {
				v.rhs = rhsLiteral.Name
			} else if rhsColumnRef, ok := rhs.(*where_clause.ColumnRef); ok {
				v.rhs = rhsColumnRef.ColumnName
			} else {
				v.rhs = ""
			}
		}
	}
	// skip transformation in the case of strict IP address
	if !strings.Contains(v.rhs, "/") {
		return where_clause.NewInfixOp(lhs.(where_clause.Statement), e.Op, rhs.(where_clause.Statement))
	}
	mappedType := v.cfg[v.tableName].TypeMappings[v.lhs]
	if mappedType != "ip" {
		return where_clause.NewInfixOp(lhs.(where_clause.Statement), e.Op, rhs.(where_clause.Statement))
	}
	if len(v.lhs) == 0 || len(v.rhs) == 0 {
		return where_clause.NewInfixOp(lhs.(where_clause.Statement), e.Op, rhs.(where_clause.Statement))
	}
	v.op = e.Op
	if v.op != "=" && v.op != "iLIKE" {
		logger.Warn().Msgf("ip transformation omitted, operator is not = or iLIKE: %s, lhs: %s, rhs: %s", v.op, v.lhs, v.rhs)
		return where_clause.NewInfixOp(lhs.(where_clause.Statement), e.Op, rhs.(where_clause.Statement))
	}
	v.rhs = strings.Replace(v.rhs, "%", "", -1)
	transformedWhereClause := &where_clause.Function{
		Name: where_clause.Literal{Name: isIPAddressInRangePrimitive},
		Args: []where_clause.Statement{
			&where_clause.Function{
				Name: where_clause.Literal{Name: CASTPrimitive},
				Args: []where_clause.Statement{
					&where_clause.Literal{Name: v.lhs},
					&where_clause.Literal{Name: StringLiteral},
				},
			},
			&where_clause.Literal{Name: v.rhs},
		},
	}
	return transformedWhereClause
}

func (v *WhereVisitor) VisitPrefixOp(e *where_clause.PrefixOp) interface{} {
	for _, arg := range e.Args {
		if arg != nil {
			arg.Accept(v)
		}
	}
	return where_clause.NewPrefixOp(e.Op, e.Args)
}

func (v *WhereVisitor) VisitFunction(e *where_clause.Function) interface{} {
	for _, arg := range e.Args {
		if arg != nil {
			arg.Accept(v)
		}
	}
	return where_clause.NewFunction(e.Name.Name, e.Args...)
}

func (v *WhereVisitor) VisitColumnRef(e *where_clause.ColumnRef) interface{} {
	return where_clause.NewColumnRef(e.ColumnName)
}

func (v *WhereVisitor) VisitNestedProperty(e *where_clause.NestedProperty) interface{} {
	ColumnRef := e.ColumnRef.Accept(v).(where_clause.ColumnRef)
	Property := e.PropertyName.Accept(v).(where_clause.Literal)
	return where_clause.NewNestedProperty(ColumnRef, Property)
}

func (v *WhereVisitor) VisitArrayAccess(e *where_clause.ArrayAccess) interface{} {
	e.ColumnRef.Accept(v)
	e.Index.Accept(v)
	return where_clause.NewArrayAccess(e.ColumnRef, e.Index)
}

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

	query.WhereClause = transformedWhereClause.(where_clause.Statement)

	return query, nil
}

func (s *SchemaCheckPass) Transform(queries []*model.Query) ([]*model.Query, error) {
	for k, query := range queries {
		var err error
		logger.Info().Msgf("IpTransformation input query: %s", query.String(context.Background()))
		query, err = s.applyIpTransformations(query)
		logger.Info().Msgf("IpTransformation output query: %s", query.String(context.Background()))
		if err != nil {
			return nil, err
		}
		queries[k] = query
	}
	return queries, nil
}
