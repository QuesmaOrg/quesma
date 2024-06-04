package model

import (
	"context"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/queryparser/where_clause"
)

type SimpleQuery struct {
	WhereClause where_clause.Statement
	OrderBy     []SelectColumn
	CanParse    bool
	FieldName   string
}

var asString = where_clause.StringRenderer{}

func (s *SimpleQuery) WhereClauseAsString() string {
	if s.WhereClause == nil {
		return ""
	}
	return (s.WhereClause).Accept(&asString).(string)
}

func NewSimpleQuery(whereClause where_clause.Statement, canParse bool) SimpleQuery {
	return SimpleQuery{WhereClause: whereClause, CanParse: canParse}
}

func NewSimpleQueryWithFieldName(whereClause where_clause.Statement, canParse bool, fieldName string) SimpleQuery {
	return SimpleQuery{WhereClause: whereClause, CanParse: canParse, FieldName: fieldName}
}

func CombineWheres(ctx context.Context, where1, where2 SimpleQuery) SimpleQuery {
	var combinedWhereClause where_clause.Statement
	if where1.WhereClause != nil && where2.WhereClause != nil {
		combinedWhereClause = where_clause.NewInfixOp(where1.WhereClause, "AND", where2.WhereClause)
	} else if where1.WhereClause != nil {
		combinedWhereClause = where1.WhereClause
	} else if where2.WhereClause != nil {
		combinedWhereClause = where2.WhereClause
	}
	combined := SimpleQuery{
		WhereClause: combinedWhereClause,
		CanParse:    where1.CanParse && where2.CanParse,
	}
	if len(where1.FieldName) > 0 && len(where2.FieldName) > 0 && where1.FieldName != where2.FieldName {
		logger.WarnWithCtx(ctx).Msgf("combining 2 where clauses with different field names: %s, %s, where queries: %v %v", where1.FieldName, where2.FieldName, where1, where2)
	}
	if len(where1.FieldName) > 0 {
		combined.FieldName = where1.FieldName
	} else {
		combined.FieldName = where2.FieldName
	}
	return combined
}
