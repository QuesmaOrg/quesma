// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/quesma/config"
	"quesma/schema"
	"strings"
)

func (s *SchemaCheckPass) applyBooleanLiteralLowering(query *model.Query) (*model.Query, error) {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitLiteral = func(b *model.BaseExprVisitor, e model.LiteralExpr) interface{} {
		if boolLiteral, ok := e.Value.(string); ok {
			// TODO this is a hack for now
			// bool literals are quoted in the query and become strings
			// we need to convert them back to bool literals
			// proper solution would require introducing a new type for bool literals in the model
			// and updating the parser to recognize them
			// but this would require much more work
			if strings.Contains(boolLiteral, "true") || strings.Contains(boolLiteral, "false") {
				boolLiteral = strings.TrimLeft(boolLiteral, "'")
				boolLiteral = strings.TrimRight(boolLiteral, "'")
				return model.NewLiteral(boolLiteral)
			}
		}
		return model.NewLiteral(e.Value)
	}

	expr := query.SelectCommand.Accept(visitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}

type SchemaCheckPass struct {
	cfg            map[string]config.IndexConfiguration
	schemaRegistry schema.Registry
	logManager     *clickhouse.LogManager
	indexMappings  map[string]config.IndexMappingsConfiguration
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
	fromTable := getFromTable(query.TableName)

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		const isIPAddressInRangePrimitive = "isIPAddressInRange"
		const CASTPrimitive = "CAST"
		const StringLiteral = "'String'"
		var lhs, rhs interface{}
		lhsValue := ""
		rhsValue := ""
		opValue := ""
		if e.Left != nil {
			lhs = e.Left.Accept(b)
			if lhs != nil {
				if lhsLiteral, ok := lhs.(model.LiteralExpr); ok {
					if asString, ok := lhsLiteral.Value.(string); ok {
						lhsValue = asString
					}
				} else if lhsColumnRef, ok := lhs.(model.ColumnRef); ok {
					lhsValue = lhsColumnRef.ColumnName
				}
			}
		}
		if e.Right != nil {
			rhs = e.Right.Accept(b)
			if rhs != nil {
				if rhsLiteral, ok := rhs.(model.LiteralExpr); ok {
					if asString, ok := rhsLiteral.Value.(string); ok {
						rhsValue = asString
					}
				} else if rhsColumnRef, ok := rhs.(model.ColumnRef); ok {
					rhsValue = rhsColumnRef.ColumnName
				}
			}
		}
		// skip transformation in the case of strict IP address
		if !strings.Contains(rhsValue, "/") {
			return model.NewInfixExpr(lhs.(model.Expr), e.Op, rhs.(model.Expr))
		}
		dataScheme, found := s.schemaRegistry.FindSchema(schema.TableName(fromTable))
		if !found {
			logger.Error().Msgf("Schema for table %s not found, this should never happen here", fromTable)
		}

		field, found := dataScheme.ResolveField(lhsValue)
		if !found {
			logger.Error().Msgf("Field %s not found in schema for table %s, should never happen here", lhsValue, fromTable)
		}
		if !field.Type.Equal(schema.TypeIp) {
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

	visitor.OverrideVisitArrayAccess = func(b *model.BaseExprVisitor, e model.ArrayAccess) interface{} {
		e.ColumnRef.Accept(b)
		e.Index.Accept(b)
		return model.NewArrayAccess(e.ColumnRef, e.Index)
	}

	expr := query.SelectCommand.Accept(visitor)

	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}

func (s *SchemaCheckPass) applyGeoTransformations(query *model.Query) (*model.Query, error) {
	fromTable := getFromTable(query.TableName)
	visitor := model.NewBaseVisitor()
	visitor.OverrideVisitSelectCommand = func(b *model.BaseExprVisitor, e model.SelectCommand) interface{} {
		if s.schemaRegistry == nil {
			logger.Error().Msg("Schema registry is not set")
			return e
		}
		schemaInstance, exists := s.schemaRegistry.FindSchema(schema.TableName(fromTable))
		if !exists {
			logger.Error().Msgf("Schema fot table %s not found", fromTable)
			return e
		}
		var groupBy []model.Expr
		for _, expr := range e.GroupBy {
			groupByExpr := expr.Accept(b).(model.Expr)
			if col, ok := expr.(model.ColumnRef); ok {
				// This checks if the column is of type point
				// and if it is, it appends the lat and lon columns to the group by clause
				field := schemaInstance.Fields[schema.FieldName(col.ColumnName)]
				if field.Type.Name == schema.TypePoint.Name {
					// TODO suffixes ::lat, ::lon are hardcoded for now
					groupBy = append(groupBy, model.NewColumnRef(field.InternalPropertyName.AsString()+"::lat"))
					groupBy = append(groupBy, model.NewColumnRef(field.InternalPropertyName.AsString()+"::lon"))
				} else {
					groupBy = append(groupBy, groupByExpr)
				}
			} else {
				groupBy = append(groupBy, groupByExpr)
			}
		}
		var columns []model.Expr
		for _, expr := range e.Columns {
			if col, ok := expr.(model.ColumnRef); ok {
				// This checks if the column is of type point
				// and if it is, it appends the lat and lon columns to the select clause
				field := schemaInstance.Fields[schema.FieldName(col.ColumnName)]
				if field.Type.Name == schema.TypePoint.Name {
					// TODO suffixes ::lat, ::lon are hardcoded for now
					columns = append(columns, model.NewColumnRef(field.InternalPropertyName.AsString()+"::lat"))
					columns = append(columns, model.NewColumnRef(field.InternalPropertyName.AsString()+"::lon"))
				} else {
					columns = append(columns, expr.Accept(b).(model.Expr))
				}
			} else {
				columns = append(columns, expr.Accept(b).(model.Expr))
			}
		}

		var fromClause model.Expr
		if e.FromClause != nil {
			fromClause = e.FromClause.Accept(b).(model.Expr)
		}

		return model.NewSelectCommand(columns, groupBy, e.OrderBy,
			fromClause, e.WhereClause, e.LimitBy, e.Limit, e.SampleLimit, e.IsDistinct, e.CTEs)
	}

	expr := query.SelectCommand.Accept(visitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}

func (s *SchemaCheckPass) applyArrayTransformations(query *model.Query) (*model.Query, error) {
	fromTable := getFromTable(query.TableName)

	table := s.logManager.FindTable(fromTable)
	if table == nil {
		logger.Error().Msgf("Table %s not found", fromTable)
		return query, nil
	}

	arrayTypeResolver := arrayTypeResolver{table: table}

	// check if the query has array columns

	selectCommand := query.SelectCommand

	var allColumns []model.ColumnRef
	for _, expr := range selectCommand.Columns {
		allColumns = append(allColumns, model.GetUsedColumns(expr)...)
	}
	if selectCommand.WhereClause != nil {
		allColumns = append(allColumns, model.GetUsedColumns(selectCommand.WhereClause)...)
	}

	hasArrayColumn := false
	for _, col := range allColumns {
		dbType := arrayTypeResolver.dbColumnType(col.ColumnName)
		if strings.HasPrefix(dbType, "Array") {
			hasArrayColumn = true
			break
		}
	}
	// no array columns, no need to transform
	if !hasArrayColumn {
		return query, nil
	}

	visitor := NewArrayTypeVisitor(arrayTypeResolver)

	expr := query.SelectCommand.Accept(visitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}

func (s *SchemaCheckPass) Transform(queries []*model.Query) ([]*model.Query, error) {
	for k, query := range queries {
		var err error
		transformationChain := []struct {
			TransformationName string
			Transformation     func(*model.Query) (*model.Query, error)
		}{
			{TransformationName: "IndexMappingQueryRewriter", Transformation: s.applyIndexMappingTransformations},
			{TransformationName: "BooleanLiteralTransformation", Transformation: s.applyBooleanLiteralLowering},
			{TransformationName: "IpTransformation", Transformation: s.applyIpTransformations},
			{TransformationName: "GeoTransformation", Transformation: s.applyGeoTransformations},
			{TransformationName: "ArrayTransformation", Transformation: s.applyArrayTransformations},
		}
		for _, transformation := range transformationChain {
			inputQuery := query.SelectCommand.String()
			query, err = transformation.Transformation(query)
			if query.SelectCommand.String() != inputQuery {

				query.TransformationHistory.SchemaTransformers = append(query.TransformationHistory.SchemaTransformers, transformation.TransformationName)

				logger.Info().Msgf(transformation.TransformationName+" triggered, input query: %s", inputQuery)
				logger.Info().Msgf(transformation.TransformationName+" triggered, output query: %s", query.SelectCommand.String())
			}
			if err != nil {
				return nil, err
			}
		}
		queries[k] = query
	}
	return queries, nil
}

type GeoIpResultTransformer struct {
	schemaRegistry schema.Registry
	fromTable      string
}

func (g *GeoIpResultTransformer) Transform(result [][]model.QueryResultRow) ([][]model.QueryResultRow, error) {
	if g.schemaRegistry == nil {
		logger.Error().Msg("Schema registry is not set")
		return result, nil
	}
	schemaInstance, exists := g.schemaRegistry.FindSchema(schema.TableName(g.fromTable))
	if !exists {
		logger.Error().Msgf("Schema fot table %s not found", g.fromTable)
		return result, nil
	}
	for i, rows := range result {
		for j, row := range rows {
			for k, col := range row.Cols {
				if strings.Contains(col.ColName, "::lat") {
					colType := schemaInstance.Fields[schema.FieldName(strings.TrimSuffix(col.ColName, "::lat"))].Type
					result[i][j].Cols[k].ColType = colType
				}
				if strings.Contains(col.ColName, "::lon") {
					colType := schemaInstance.Fields[schema.FieldName(strings.TrimSuffix(col.ColName, "::lon"))].Type
					result[i][j].Cols[k].ColType = colType
				}
			}
		}
	}
	return result, nil
}
