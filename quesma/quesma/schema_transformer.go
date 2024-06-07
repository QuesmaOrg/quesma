package quesma

import (
	"context"
	"errors"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/where_clause"
	"mitmproxy/quesma/quesma/config"
	"strings"
)

type WhereVisitor struct {
	lhs string
	rhs string
	op  string
}

func (v *WhereVisitor) VisitLiteral(e *where_clause.Literal) interface{} {
	return where_clause.NewLiteral(e.Name)
}

func (v *WhereVisitor) VisitInfixOp(e *where_clause.InfixOp) interface{} {
	lhs := e.Left.Accept(v).(where_clause.Statement)
	rhs := e.Right.Accept(v).(where_clause.Statement)
	if lhs != nil {
		if lhsLiteral, ok := lhs.(*where_clause.Literal); ok {
			v.lhs = lhsLiteral.Name
		}
	}
	if rhs != nil {
		if rhsLiteral, ok := rhs.(*where_clause.Literal); ok {
			v.rhs = rhsLiteral.Name
		}
	}
	v.op = e.Op
	return where_clause.NewInfixOp(lhs, e.Op, rhs)
}

func (v *WhereVisitor) VisitPrefixOp(e *where_clause.PrefixOp) interface{} {
	return where_clause.NewPrefixOp(e.Op, e.Args)
}

func (v *WhereVisitor) VisitFunction(e *where_clause.Function) interface{} {
	return where_clause.NewFunction(e.Name.Name, e.Args...)
}

func (v *WhereVisitor) VisitColumnRef(e *where_clause.ColumnRef) interface{} {
	return where_clause.NewColumnRef(e.ColumnName)
}

func (v *WhereVisitor) VisitNestedProperty(e *where_clause.NestedProperty) interface{} {
	return where_clause.NewNestedProperty(e.ColumnRef, e.PropertyName)
}

func (v *WhereVisitor) VisitArrayAccess(e *where_clause.ArrayAccess) interface{} {
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
	const isIPAddressInRangePrimitive = "isIPAddressInRange"
	const CASTPrimitive = "CAST"
	const StringLiteral = "'String'"
	if query.WhereClause == nil {
		return query, nil
	}
	whereVisitor := &WhereVisitor{}
	query.WhereClause.Accept(whereVisitor)

	fromTable := getFromTable(query.TableName)

	mappedType := s.cfg[fromTable].TypeMappings[strings.Trim(whereVisitor.lhs, "\"")]
	if mappedType != "ip" {
		return query, nil
	}
	if len(whereVisitor.lhs) == 0 || len(whereVisitor.rhs) == 0 {
		return query, errors.New("schema transformation failed, lhs or rhs is empty")
	}
	if whereVisitor.op != "=" && whereVisitor.op != "iLIKE" && whereVisitor.op != "IN" {
		logger.Warn().Msg("ip transformation omitted, operator is not =")
		return query, nil
	}
	whereVisitor.rhs = strings.Replace(whereVisitor.rhs, "%", "", -1)

	var transformedWhereClause where_clause.Statement

	switch whereVisitor.op {
	case "IN":
		transformedWhereClause = query.WhereClause
	default:
		transformedWhereClause = &where_clause.Function{
			Name: where_clause.Literal{Name: isIPAddressInRangePrimitive},
			Args: []where_clause.Statement{
				&where_clause.Function{
					Name: where_clause.Literal{Name: CASTPrimitive},
					Args: []where_clause.Statement{
						&where_clause.Literal{Name: whereVisitor.lhs},
						&where_clause.Literal{Name: StringLiteral},
					},
				},
				&where_clause.Literal{Name: whereVisitor.rhs},
			},
		}

	}

	query.WhereClause = transformedWhereClause
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
