// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	"fmt"
	"quesma/logger"
	"quesma/model"
	"quesma/model/typical_queries"
	"quesma/quesma/config"
	"quesma/schema"
	"sort"
	"strings"
)

type SchemaCheckPass struct {
	cfg            map[string]config.IndexConfiguration
	schemaRegistry schema.Registry
}

func (s *SchemaCheckPass) applyBooleanLiteralLowering(index schema.Schema, query *model.Query) (*model.Query, error) {

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

// Below function applies schema transformations to the query regarding ip addresses.
// Internally, it converts sql statement like
// SELECT * FROM "kibana_sample_data_logs" WHERE lhs op rhs
// where op is '=' or 'iLIKE'
// into
// SELECT * FROM "kibana_sample_data_logs" WHERE isIPAddressInRange(CAST(COALESCE(lhs,'0.0.0.0') AS "String"),rhs) - COALESCE is used to handle NULL values
//
//	e.g.: isIPAddressInRange(CAST(COALESCE(IP_ADDR_COLUMN_NAME,'0.0.0.0') AS "String"),'10.10.10.0/24')
func (s *SchemaCheckPass) applyIpTransformations(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {

	fromTable := query.TableName

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

		field, found := indexSchema.ResolveFieldByInternalName(lhsValue)
		if !found {
			logger.Error().Msgf("Field %s not found in schema for table %s, should never happen here", lhsValue, fromTable)
		}
		if !field.Type.Equal(schema.QuesmaTypeIp) {
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

func (s *SchemaCheckPass) applyGeoTransformations(currentSchema schema.Schema, query *model.Query) (*model.Query, error) {
	fromTable := query.TableName

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
				if field.Type.Name == schema.QuesmaTypePoint.Name {
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
				if field.Type.Name == schema.QuesmaTypePoint.Name {
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

func (s *SchemaCheckPass) applyArrayTransformations(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {

	arrayTypeResolver := arrayTypeResolver{indexSchema: indexSchema}

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

func (s *SchemaCheckPass) applyMapTransformations(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {

	mapResolver := mapTypeResolver{indexSchema: indexSchema}

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

func (s *SchemaCheckPass) applyPhysicalFromExpression(currentSchema schema.Schema, query *model.Query) (*model.Query, error) {

	if query.TableName == model.SingleTableNamePlaceHolder {
		logger.Warn().Msg("applyPhysicalFromExpression: physical table name is not set")
	}

	// TODO compute physical from expression based on single table or union or whatever ....
	physicalFromExpression := model.NewTableRef(query.TableName)

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitTableRef = func(b *model.BaseExprVisitor, e model.TableRef) interface{} {
		if e.Name == model.SingleTableNamePlaceHolder {
			return physicalFromExpression
		}
		return e
	}

	expr := query.SelectCommand.Accept(visitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}

func (s *SchemaCheckPass) applyWildcardExpansion(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {

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

		cols := make([]string, 0, len(indexSchema.Fields))
		for _, col := range indexSchema.Fields {
			if col.Type.Name == schema.QuesmaTypePoint.Name { // Temporary workaround for kibana_flights
				continue
			}
			cols = append(cols, col.InternalPropertyName.AsString())
		}
		sort.Strings(cols)

		for _, col := range cols {
			newColumns = append(newColumns, model.NewColumnRef(col))
		}
	}

	query.SelectCommand.Columns = newColumns

	return query, nil
}

func (s *SchemaCheckPass) applyFullTextField(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {

	var fullTextFields []string

	for _, field := range indexSchema.Fields {
		if field.Type.IsFullText() {
			fullTextFields = append(fullTextFields, field.InternalPropertyName.AsString())
		}
	}

	// sort the fields to ensure deterministic results
	sort.Strings(fullTextFields)

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

func (s *SchemaCheckPass) applyTimestampField(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {

	var timestampColumnName string

	// check if the schema has a timestamp field configured
	if column, ok := indexSchema.Fields[model.TimestampFieldName]; ok {
		timestampColumnName = column.InternalPropertyName.AsString()
	}

	// if not found, check if the table has a timestamp field discovered somehow
	// This is commented out for now.
	// We should be able to fetch table (physical representation) for current schema
	//
	/*
		if timestampColumnName == "" && table.DiscoveredTimestampFieldName != nil {
			timestampColumnName = *table.DiscoveredTimestampFieldName
		}
	*/
	var replacementExpr model.Expr

	if timestampColumnName == "" {
		// no timestamp field found, replace with NULL if any

		// see comment above, we don't know what is a discovered timestamp field
		//replacementExpr = model.NewLiteral("NULL")
	} else {
		// check if the query has hits type, so '_id' generation should not be based on timestamp
		//
		// This is a mess. `query.Type` holds a pointer to Hits, but Hits do not have pointer receivers to mutate the state.
		if hits, ok := query.Type.(*typical_queries.Hits); ok {
			newHits := hits.WithTimestampField(timestampColumnName)
			query.Type = &newHits
		}

		// if the target column is not the canonical timestamp field, replace it
		if timestampColumnName != model.TimestampFieldName {
			replacementExpr = model.NewColumnRef(timestampColumnName)
		}
	}

	// no replacement needed
	if replacementExpr == nil {
		return query, nil
	}

	visitor := model.NewBaseVisitor()
	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {

		// full text field should be used only in where clause
		if e.ColumnName == model.TimestampFieldName {
			return replacementExpr
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

	transformationChain := []struct {
		TransformationName string
		Transformation     func(schema.Schema, *model.Query) (*model.Query, error)
	}{
		{TransformationName: "PhysicalFromExpressionTransformation", Transformation: s.applyPhysicalFromExpression},
		{TransformationName: "FullTextFieldTransformation", Transformation: s.applyFullTextField},
		{TransformationName: "TimestampFieldTransformation", Transformation: s.applyTimestampField},
		{TransformationName: "BooleanLiteralTransformation", Transformation: s.applyBooleanLiteralLowering},
		{TransformationName: "IpTransformation", Transformation: s.applyIpTransformations},
		{TransformationName: "GeoTransformation", Transformation: s.applyGeoTransformations},
		{TransformationName: "ArrayTransformation", Transformation: s.applyArrayTransformations},
		{TransformationName: "MapTransformation", Transformation: s.applyMapTransformations},
		{TransformationName: "WildcardExpansion", Transformation: s.applyWildcardExpansion},
	}

	for k, query := range queries {
		var err error

		indexSchema, ok := s.schemaRegistry.FindSchema(schema.TableName(query.TableName))
		if !ok {
			return nil, fmt.Errorf("schema not found: %s", query.TableName)
		}

		for _, transformation := range transformationChain {

			inputQuery := query.SelectCommand.String()
			query, err = transformation.Transformation(indexSchema, query)
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
