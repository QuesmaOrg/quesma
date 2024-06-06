package quesma

import (
	"errors"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/where_clause"
	"mitmproxy/quesma/quesma/config"
	"strconv"
	"strings"
)

type WhereVisitor struct {
	lhs string
	rhs string
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
		v.rhs = rhs.(string)
	}
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

func (s *SchemaCheckPass) applyIpTransformations(query model.Query) (model.Query, error) {
	const isIPAddressInRangePrimitive = "isIPAddressInRange"
	const CASTPrimitive = "CAST"
	const StringLiteral = "'String'"

	if query.WhereClause == nil {
		return query, nil
	}
	whereVisitor := &WhereVisitor{}
	query.WhereClause.Accept(whereVisitor)
	fromTable := strings.Trim(query.FromClause, "\"")
	mappedType := s.cfg[fromTable].TypeMappings[strings.Trim(whereVisitor.lhs, "\"")]
	if mappedType != "ip" {
		return query, nil
	}
	if len(whereVisitor.lhs) == 0 || len(whereVisitor.rhs) == 0 {
		return query, errors.New("schema transformation failed, lhs or rhs is empty")
	}
	transformedWhereClause := &where_clause.Function{
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
	query.WhereClause = transformedWhereClause
	return query, nil
}

func (s *SchemaCheckPass) Transform(queries []model.Query) ([]model.Query, error) {
	for k, query := range queries {
		var err error
		query, err = s.applyIpTransformations(query)
		if err != nil {
			return nil, err
		}
		queries[k] = query
	}
	return queries, nil
}
