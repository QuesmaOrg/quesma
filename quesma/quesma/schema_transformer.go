package quesma

import (
	"context"
	"errors"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/where_clause"
	"mitmproxy/quesma/quesma/config"
	"strconv"
	"strings"
)

type WhereVisitor struct {
	lhs string
	rhs []string
	op  string
}

func (v *WhereVisitor) VisitLiteral(e *where_clause.Literal) interface{} {
	return e.Name
}

func (v *WhereVisitor) VisitInfixOp(e *where_clause.InfixOp) interface{} {
	if e.Left != nil {
		lhs := e.Left.Accept(v)
		v.lhs = lhs.(string)
	}
	if e.Right != nil {
		rhs := e.Right.Accept(v)
		v.rhs = append(v.rhs, rhs.(string))
	}
	v.op = e.Op
	return ""
}

func (v *WhereVisitor) VisitPrefixOp(*where_clause.PrefixOp) interface{} {
	return ""
}

func (v *WhereVisitor) VisitFunction(*where_clause.Function) interface{} {
	return ""
}

func (v *WhereVisitor) VisitColumnRef(e *where_clause.ColumnRef) interface{} {
	return strconv.Quote(e.ColumnName)
}

func (v *WhereVisitor) VisitNestedProperty(*where_clause.NestedProperty) interface{} {
	return ""
}

func (v *WhereVisitor) VisitArrayAccess(*where_clause.ArrayAccess) interface{} {
	return ""
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
func (s *SchemaCheckPass) applyIpTransformations(query model.Query) (model.Query, error) {
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
	for i, rhs := range whereVisitor.rhs {
		rhs = strings.Replace(rhs, "%", "", -1)
		whereVisitor.rhs[i] = rhs
	}

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
				&where_clause.Literal{Name: whereVisitor.rhs[0]},
			},
		}

	}

	query.WhereClause = transformedWhereClause
	return query, nil
}

func (s *SchemaCheckPass) Transform(queries []model.Query) ([]model.Query, error) {
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
