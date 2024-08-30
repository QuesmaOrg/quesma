// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"fmt"
	"quesma/catch_all_logs"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/quesma/config"
	"quesma/schema"
	"sort"
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
}

// This functions trims the db name from the table name if exists
// We need to do this due to the way we are storing the schema in the config
// TableMap is indexed by table name, not db.table name
func getFromTable(fromTable string) string {
	// cut db name from table name if exists
	/*
		if idx := strings.IndexByte(fromTable, '.'); idx >= 0 {
			fromTable = fromTable[idx:]
			fromTable = strings.Trim(fromTable, ".")
		}

	*/
	return strings.Trim(fromTable, "\"")
}

// Below function applies schema transformations to the query regarding ip addresses.
// Internally, it converts sql statement like
// SELECT * FROM "kibana_sample_data_logs" WHERE lhs op rhs
// where op is '=' or 'iLIKE'
// into
// SELECT * FROM "kibana_sample_data_logs" WHERE isIPAddressInRange(CAST(COALESCE(lhs,'0.0.0.0') AS "String"),rhs) - COALESCE is used to handle NULL values
//
//	e.g.: isIPAddressInRange(CAST(COALESCE(IP_ADDR_COLUMN_NAME,'0.0.0.0') AS "String"),'10.10.10.0/24')
func (s *SchemaCheckPass) applyIpTransformations(query *model.Query) (*model.Query, error) {
	fromTable := getFromTable(query.TableName)

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		const isIPAddressInRangePrimitive = "isIPAddressInRange"
		const CASTPrimitive = "CAST"
		const COALESCEPrimitive = "COALESCE"
		const StringLiteral = "String"
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

		field, found := dataScheme.ResolveFieldByInternalName(lhsValue)
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
						&model.AliasedExpr{
							Expr: &model.FunctionExpr{
								Name: COALESCEPrimitive,
								Args: []model.Expr{
									lhs.(model.Expr),
									&model.LiteralExpr{Value: "'0.0.0.0'"},
								},
							},
							Alias: StringLiteral,
						},
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
			fromClause, e.WhereClause, e.LimitBy, e.Limit, e.SampleLimit, e.IsDistinct, e.CTEs, e.NamedCTEs)
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

func (s *SchemaCheckPass) applyMapTransformations(query *model.Query) (*model.Query, error) {
	fromTable := getFromTable(query.TableName)

	table := s.logManager.FindTable(fromTable)
	if table == nil {
		logger.Error().Msgf("Table %s not found", fromTable)
		return query, nil
	}

	mapResolver := mapTypeResolver{table: table}

	// check if the query has map columns

	selectCommand := query.SelectCommand

	var allColumns []model.ColumnRef
	for _, expr := range selectCommand.Columns {
		allColumns = append(allColumns, model.GetUsedColumns(expr)...)
	}
	if selectCommand.WhereClause != nil {
		allColumns = append(allColumns, model.GetUsedColumns(selectCommand.WhereClause)...)
	}

	hasMapColumn := false
	for _, col := range allColumns {
		isMap, _, _ := mapResolver.isMap(col.ColumnName)
		if isMap {
			hasMapColumn = true
			break
		}
	}
	// no array columns, no need to transform
	if !hasMapColumn {
		return query, nil
	}

	visitor := NewMapTypeVisitor(mapResolver)

	expr := query.SelectCommand.Accept(visitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}

func (s *SchemaCheckPass) applyPhysicalFromExpression(query *model.Query) (*model.Query, error) {

	if query.TableName == model.SingleTableNamePlaceHolder {
		logger.Warn().Msg("applyPhysicalFromExpression: physical table name is not set")
	}

	// TODO compute physical from expression based on single table or union or whatever ....
	var physicalFromExpression model.Expr
	if catch_all_logs.Enabled {
		physicalFromExpression = model.NewTableRef(catch_all_logs.TableName)
	} else {
		physicalFromExpression = model.NewTableRef(query.TableName)
	}

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitTableRef = func(b *model.BaseExprVisitor, e model.TableRef) interface{} {
		if e.Name == model.SingleTableNamePlaceHolder {
			return physicalFromExpression
		}
		return e
	}

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
		if catch_all_logs.Enabled {
			if e.ColumnName == "timestamp" || e.ColumnName == "epoch_time" || e.ColumnName == `"epoch_time"` {
				return model.NewColumnRef("@timestamp")
			}
		}
		return e
	}

	visitor.OverrideVisitSelectCommand = func(b *model.BaseExprVisitor, selectStm model.SelectCommand) interface{} {
		var columns, groupBy []model.Expr
		var orderBy []model.OrderByExpr
		from := selectStm.FromClause
		where := selectStm.WhereClause

		for _, expr := range selectStm.Columns {
			columns = append(columns, expr.Accept(b).(model.Expr))
		}
		for _, expr := range selectStm.GroupBy {
			groupBy = append(groupBy, expr.Accept(b).(model.Expr))
		}
		for _, expr := range selectStm.OrderBy {
			orderBy = append(orderBy, expr.Accept(b).(model.OrderByExpr))
		}
		if selectStm.FromClause != nil {
			from = selectStm.FromClause.Accept(b).(model.Expr)
		}
		if selectStm.WhereClause != nil {
			where = selectStm.WhereClause.Accept(b).(model.Expr)
		}

		if catch_all_logs.Enabled {

			pattern := query.IndexPattern
			pattern = strings.ReplaceAll(pattern, "*", "%")

			indexWhere := model.NewInfixExpr(model.NewColumnRef(catch_all_logs.IndexNameColumn), "ILIKE", model.NewLiteral(fmt.Sprintf("'%s'", pattern)))

			if selectStm.WhereClause != nil {
				where = model.And([]model.Expr{selectStm.WhereClause.Accept(b).(model.Expr), indexWhere})
			} else {
				where = indexWhere
			}
		}

		var ctes []*model.SelectCommand
		if selectStm.CTEs != nil {
			ctes = make([]*model.SelectCommand, 0)
			for _, cte := range selectStm.CTEs {
				ctes = append(ctes, cte.Accept(b).(*model.SelectCommand))
			}
		}
		var namedCTEs []*model.CTE
		if selectStm.NamedCTEs != nil {
			for _, cte := range selectStm.NamedCTEs {
				namedCTEs = append(namedCTEs, cte.Accept(b).(*model.CTE))
			}
		}

		return model.NewSelectCommand(columns, groupBy, orderBy, from, where, selectStm.LimitBy, selectStm.Limit, selectStm.SampleLimit, selectStm.IsDistinct, ctes, namedCTEs)
	}

	expr := query.SelectCommand.Accept(visitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	} else {

	}

	return query, nil
}

func (s *SchemaCheckPass) applyWildcardExpansion(query *model.Query) (*model.Query, error) {
	fromTable := getFromTable(query.TableName)

	table := s.logManager.FindTable(fromTable)
	if table == nil {
		logger.Error().Msgf("Table %s not found", fromTable)
		return query, nil
	}

	var newColumns []model.Expr
	var hasWildcard bool

	for _, selectColumn := range query.SelectCommand.Columns {

		if selectColumn == model.NewWildcardExpr {
			hasWildcard = true
		} else {
			newColumns = append(newColumns, selectColumn)
		}
	}

	if hasWildcard {

		cols := make([]string, 0, len(table.Cols))
		for _, col := range table.Cols {
			cols = append(cols, col.Name)
		}
		sort.Strings(cols)

		for _, col := range cols {
			newColumns = append(newColumns, model.NewColumnRef(col))
		}
	}

	query.SelectCommand.Columns = newColumns

	return query, nil
}

func (s *SchemaCheckPass) applyFullTextField(query *model.Query) (*model.Query, error) {
	fromTable := getFromTable(query.TableName)

	// FIXME we should use the schema registry here
	//
	table := s.logManager.FindTable(fromTable)
	if table == nil {
		logger.Error().Msgf("Table %s not found", fromTable)
		return query, nil
	}
	fullTextFields := table.GetFulltextFields()

	visitor := model.NewBaseVisitor()

	var err error

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {

		// full text field should be used only in where clause
		if e.ColumnName == model.FullTextFieldNamePlaceHolder {
			err = fmt.Errorf("full text field name placeholder found in query")
		}
		return e
	}

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		col, ok := e.Left.(model.ColumnRef)
		if ok {
			if col.ColumnName == model.FullTextFieldNamePlaceHolder {

				if len(fullTextFields) == 0 {
					return model.NewLiteral(false)
				}

				var expressions []model.Expr

				for _, field := range fullTextFields {
					expressions = append(expressions, model.NewInfixExpr(model.NewColumnRef(field), e.Op, e.Right))
				}

				res := model.Or(expressions)
				return res
			}
		}

		return model.NewInfixExpr(e.Left.Accept(b).(model.Expr), e.Op, e.Right.Accept(b).(model.Expr))
	}

	expr := query.SelectCommand.Accept(visitor)

	if err != nil {
		return nil, err
	}

	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil

}

func (s *SchemaCheckPass) checkDottedColumns(query *model.Query) (*model.Query, error) {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {

		if strings.Contains(e.ColumnName, ".") {
			fmt.Println("XXX Dotted column found: ", e.ColumnName)
			return model.NewColumnRef(strings.ReplaceAll(e.ColumnName, ".", "::"))
		}

		return e
	}

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
			{TransformationName: "PhysicalFromExpressionTransformation", Transformation: s.applyPhysicalFromExpression},
			{TransformationName: "FullTextFieldTransformation", Transformation: s.applyFullTextField},
			{TransformationName: "BooleanLiteralTransformation", Transformation: s.applyBooleanLiteralLowering},
			{TransformationName: "IpTransformation", Transformation: s.applyIpTransformations},
			{TransformationName: "GeoTransformation", Transformation: s.applyGeoTransformations},
			{TransformationName: "ArrayTransformation", Transformation: s.applyArrayTransformations},
			{TransformationName: "MapTransformation", Transformation: s.applyMapTransformations},
			{TransformationName: "WildcardExpansion", Transformation: s.applyWildcardExpansion},
			{TransformationName: "DottedColumns", Transformation: s.checkDottedColumns},
		}
		for _, transformation := range transformationChain {
			inputQuery := query.SelectCommand.String()
			query, err = transformation.Transformation(query)
			if err != nil {
				return nil, err
			}
			if query.SelectCommand.String() != inputQuery {

				query.TransformationHistory.SchemaTransformers = append(query.TransformationHistory.SchemaTransformers, transformation.TransformationName)

				logger.Info().Msgf(transformation.TransformationName+" triggered, input query: %s", inputQuery)
				logger.Info().Msgf(transformation.TransformationName+" triggered, output query: %s", query.SelectCommand.String())
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
