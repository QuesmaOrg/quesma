// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package frontend_connectors

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/model/typical_queries"
	"github.com/QuesmaOrg/quesma/platform/parsers/elastic_query_dsl"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/transformations"
	"sort"
	"strconv"
	"strings"
)

type SchemaCheckPass struct {
	cfg                 *config.QuesmaConfiguration
	tableDiscovery      database_common.TableDiscovery
	searchAfterStrategy searchAfterStrategy
}

func NewSchemaCheckPass(cfg *config.QuesmaConfiguration, tableDiscovery database_common.TableDiscovery, strategyType searchAfterStrategyType) *SchemaCheckPass {
	return &SchemaCheckPass{
		cfg:                 cfg,
		tableDiscovery:      tableDiscovery,
		searchAfterStrategy: searchAfterStrategyFactory(strategyType),
	}
}

func (s *SchemaCheckPass) isFieldMapSyntaxEnabled(query *model.Query) bool {

	var enabled bool

	if len(query.Indexes) == 1 {
		if indexConf, ok := s.cfg.IndexConfig[query.Indexes[0]]; ok {
			enabled = indexConf.EnableFieldMapSyntax
		}
	}

	return enabled
}

func (s *SchemaCheckPass) ApplySelectFromCluster(_ schema.Schema, query *model.Query) (*model.Query, error) {
	clusterName := s.cfg.ClusterName
	if clusterName == "" {
		return query, nil
	}
	logger.Info().Msgf("Applying select from cluster query: %v", query.SelectCommand.FromClause)

	clusterLiteral := func(dbName, tableName string) model.LiteralExpr {
		return model.NewLiteral(fmt.Sprintf("cluster(%s, %s, %s)", strconv.Quote(clusterName), strconv.Quote(dbName), strconv.Quote(tableName)))
	}

	var visitExpr func(expr model.Expr) model.Expr
	visitExpr = func(expr model.Expr) model.Expr {
		switch e := expr.(type) {
		case model.TableRef:
			return clusterLiteral(e.DatabaseName, e.Name)
		case *model.TableRef:
			newVal := clusterLiteral(e.DatabaseName, e.Name)
			return &newVal
		case *model.SelectCommand:
			newSelect := *e
			if e.FromClause != nil {
				newSelect.FromClause = visitExpr(e.FromClause)
			}
			return &newSelect
		case model.SelectCommand:
			newSelect := e
			if e.FromClause != nil {
				newSelect.FromClause = visitExpr(e.FromClause)
			}
			return newSelect
		default:
			return expr
		}
	}

	if query.SelectCommand.FromClause != nil {
		query.SelectCommand.FromClause = visitExpr(query.SelectCommand.FromClause)
	}

	return query, nil
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
				var asAny any = boolLiteral
				return e.CloneAndOverride(&asAny, nil, nil)
			}
		}
		return e.Clone()
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

func (s *SchemaCheckPass) applyGeoTransformations(schemaInstance schema.Schema, query *model.Query) (*model.Query, error) {

	replace := make(map[string]model.Expr)

	for _, field := range schemaInstance.Fields {
		if field.Type.Name == schema.QuesmaTypePoint.Name {
			lon := model.NewColumnRef(field.InternalPropertyName.AsString() + "_lon")
			lat := model.NewColumnRef(field.InternalPropertyName.AsString() + "_lat")

			// This is a workaround. Clickhouse Point is defined as Tuple. We need to know the type of the tuple.
			// In this step we merge two columns into single map here. Map is in elastic format.

			// In this point we assume that Quesma point type is stored into two separate columns.
			replace[field.InternalPropertyName.AsString()] = model.NewFunction("map",
				model.NewLiteral("'lat'"),
				lat,
				model.NewLiteral("'lon'"),
				lon)

			// these a just if we need multifields support
			replace[field.InternalPropertyName.AsString()+".lat"] = lat
			replace[field.InternalPropertyName.AsString()+".lon"] = lon

			// if the point is stored as a single column, we need to extract the lat and lon
			//replace[field.PropertyName.AsString()] = model.NewFunction("give_me_point", model.NewColumnRef(field.InternalPropertyName.AsString()))
			//replace[field.PropertyName.AsString()+".lat"] = model.NewFunction("give_me_lat", model.NewColumnRef(field.InternalPropertyName.AsString()))
			//replace[field.PropertyName.AsString()+".lon"] = model.NewFunction("give_me_lon", model.NewColumnRef(field.InternalPropertyName.AsString()))

		}
	}

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
		if expr, ok := replace[e.ColumnName]; ok {
			return expr
		}
		return e
	}

	visitor.OverrideVisitFunction = func(b *model.BaseExprVisitor, e model.FunctionExpr) interface{} {

		var suffix string
		switch e.Name {
		case model.QuesmaGeoLatFunction:
			suffix = ".lat"
		case model.QuesmaGeoLonFunction:
			suffix = ".lon"
		}

		if suffix != "" && len(e.Args) == 1 {
			if col, ok := e.Args[0].(model.ColumnRef); ok {
				if expr, ok := replace[col.ColumnName+suffix]; ok {
					return expr
				}
			}
		}

		return visitFunction(b, e)
	}

	visitor.OverrideVisitSelectCommand = func(v *model.BaseExprVisitor, query model.SelectCommand) interface{} {
		var columns, groupBy []model.Expr
		var orderBy []model.OrderByExpr
		from := query.FromClause
		where := query.WhereClause

		for _, expr := range query.Columns {
			var alias string
			if col, ok := expr.(model.ColumnRef); ok {
				if _, ok := replace[col.ColumnName]; ok {
					alias = col.ColumnName
				}
			}

			col := expr.Accept(v).(model.Expr)

			if alias != "" {
				col = model.NewAliasedExpr(col, alias)
			}

			columns = append(columns, col)
		}
		for _, expr := range query.GroupBy {
			groupBy = append(groupBy, expr.Accept(v).(model.Expr))
		}
		for _, expr := range query.OrderBy {
			orderBy = append(orderBy, expr.Accept(v).(model.OrderByExpr))
		}
		if query.FromClause != nil {
			from = query.FromClause.Accept(v).(model.Expr)
		}
		if query.WhereClause != nil {
			where = query.WhereClause.Accept(v).(model.Expr)
		}

		var namedCTEs []*model.CTE
		if query.NamedCTEs != nil {
			for _, cte := range query.NamedCTEs {
				namedCTEs = append(namedCTEs, cte.Accept(v).(*model.CTE))
			}
		}

		var limitBy []model.Expr
		if query.LimitBy != nil {
			for _, expr := range query.LimitBy {
				limitBy = append(limitBy, expr.Accept(v).(model.Expr))
			}
		}
		return model.NewSelectCommand(columns, groupBy, orderBy, from, where, limitBy, query.Limit, query.SampleLimit, query.IsDistinct, namedCTEs)
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

	allColumns := model.GetUsedColumns(query.SelectCommand)

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

	var (
		visitor         model.ExprVisitor
		visitorHadError bool
	)

	if checkIfGroupingByArrayColumn(query.SelectCommand, arrayTypeResolver) {
		visitor = NewArrayJoinVisitor(arrayTypeResolver)
	} else {
		visitor, visitorHadError = NewArrayTypeVisitor(arrayTypeResolver)
	}

	expr := query.SelectCommand.Accept(visitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	if visitorHadError {
		selectAsStr := model.AsString(query.SelectCommand)
		logger.ErrorWithReason("array transformation error").Msgf("Query: %s", selectAsStr)
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

func (s *SchemaCheckPass) computeListIndexPrefixesToGroup() []string {

	const groupByCommonTableIndexes = "group_common_table_indexes"

	var groupIndexesPrefix []string
	if s.cfg.DefaultQueryOptimizers != nil {
		if opt, ok := s.cfg.DefaultQueryOptimizers[groupByCommonTableIndexes]; ok {
			if !opt.Disabled {
				for k, v := range opt.Properties {
					if v != "false" {
						groupIndexesPrefix = append(groupIndexesPrefix, k)
					}
				}
			}
		}
	}
	return groupIndexesPrefix
}

func (s *SchemaCheckPass) applyPhysicalFromExpression(currentSchema schema.Schema, query *model.Query) (*model.Query, error) {

	if query.TableName == model.SingleTableNamePlaceHolder {
		logger.Warn().Msg("applyPhysicalFromExpression: physical table name is not set")
	}

	var useCommonTable bool
	if len(query.Indexes) == 1 {
		if indexConf, ok := s.cfg.IndexConfig[query.Indexes[0]]; ok {
			useCommonTable = indexConf.UseCommonTable
		} else if s.cfg.UseCommonTableForWildcard {
			useCommonTable = true
		}
	} else { // we can handle querying multiple indexes from common table only
		useCommonTable = true
	}

	physicalFromExpression := model.NewTableRefWithDatabaseName(query.TableName, currentSchema.DatabaseName)

	if useCommonTable {
		physicalFromExpression = model.NewTableRef(common_table.TableName)
	}

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitTableRef = func(b *model.BaseExprVisitor, e model.TableRef) interface{} {
		if e.Name == model.SingleTableNamePlaceHolder {
			return physicalFromExpression
		}
		return e
	}

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
		// TODO is this nessessery?
		if useCommonTable {
			if e.ColumnName == "timestamp" || e.ColumnName == "epoch_time" || e.ColumnName == `"epoch_time"` {
				return model.NewColumnRefWithTable("@timestamp", e.TableAlias)
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

		// add filter for common table, if needed
		if useCommonTable && from == physicalFromExpression {

			orExpression := make(map[string]model.Expr)

			groupIndexesPrefix := s.computeListIndexPrefixesToGroup()

			for _, indexName := range query.Indexes {
				var added bool

				// apply optimization here
				if len(groupIndexesPrefix) > 0 {
					for _, prefix := range groupIndexesPrefix {
						if strings.HasPrefix(indexName, prefix) {
							added = true
							if _, ok := orExpression[prefix]; !ok {
								orExpression[prefix] = model.NewFunction("startsWith", model.NewColumnRef(common_table.IndexNameColumn), model.NewLiteral(fmt.Sprintf("'%s'", prefix)))
							}
						}
					}
				}

				if !added {
					orExpression[indexName] = model.NewInfixExpr(model.NewColumnRef(common_table.IndexNameColumn), "=", model.NewLiteral(fmt.Sprintf("'%s'", indexName)))
				}
			}

			// keep in the order
			var orExpressionOrder []string
			for k := range orExpression {
				orExpressionOrder = append(orExpressionOrder, k)
			}
			sort.Strings(orExpressionOrder)

			var indexWhere []model.Expr
			for _, name := range orExpressionOrder {
				indexWhere = append(indexWhere, orExpression[name])
			}

			indicesWhere := model.Or(indexWhere)

			if selectStm.WhereClause != nil {
				where = model.And([]model.Expr{selectStm.WhereClause.Accept(b).(model.Expr), indicesWhere})
			} else {
				where = indicesWhere
			}
		}

		var namedCTEs []*model.CTE
		if selectStm.NamedCTEs != nil {
			for _, cte := range selectStm.NamedCTEs {
				namedCTEs = append(namedCTEs, cte.Accept(b).(*model.CTE))
			}
		}

		return model.NewSelectCommand(columns, groupBy, orderBy, from, where, selectStm.LimitBy, selectStm.Limit, selectStm.SampleLimit, selectStm.IsDistinct, namedCTEs)
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

		if model.IsWildcard(selectColumn) {
			hasWildcard = true
		} else {
			newColumns = append(newColumns, selectColumn)
		}
	}

	if hasWildcard {

		cols := make([]string, 0, len(indexSchema.Fields))
		for _, col := range indexSchema.Fields {
			// Take only fields that are ingested
			if col.Origin == schema.FieldSourceIngest {
				cols = append(cols, col.PropertyName.AsString())
			}
		}

		if query.RuntimeMappings != nil {
			// add columns that are not in the schema but are in the runtime mappings
			// these columns  will be transformed later
			for name := range query.RuntimeMappings {
				cols = append(cols, name)
			}
		}

		sort.Strings(cols)

		for _, col := range cols {
			newColumns = append(newColumns, model.NewColumnRef(col))
		}

		if len(query.Indexes) > 1 {
			newColumns = append(newColumns, model.NewColumnRef(common_table.IndexNameColumn))
		}
	}

	if len(newColumns) == 0 {
		return nil, fmt.Errorf("applyWildcardExpansion: no columns found in the query")
	}

	query.SelectCommand.Columns = newColumns

	return query, nil
}

func (s *SchemaCheckPass) applyFullTextField(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {

	var fullTextFields []string

	for _, field := range indexSchema.Fields {
		if field.Type.IsFullText() {
			// Take only fields that are ingested
			if field.Origin == schema.FieldSourceIngest {
				fullTextFields = append(fullTextFields, field.InternalPropertyName.AsString())
			}
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
					if (strings.ToUpper(e.Op) == "LIKE" || strings.ToUpper(e.Op) == "ILIKE") && model.AsString(e.Right) == "'%'" {
						return model.TrueExpr
					}
					return model.FalseExpr
				}

				var expressions []model.Expr

				for _, field := range fullTextFields {
					colRef := model.NewColumnRefWithTable(field, col.TableAlias)
					expressions = append(expressions, model.NewInfixExpr(colRef, e.Op, e.Right))
				}

				res := model.Or(expressions)
				return res
			}
		}

		return visitInfix(b, e)
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
	table, ok := s.tableDiscovery.TableDefinitions().Load(query.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", query.TableName)
	}
	if table.DiscoveredTimestampFieldName != nil {
		timestampColumnName = *table.DiscoveredTimestampFieldName
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
	var replacementName string

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
			replacementName = timestampColumnName
		}
	}

	// no replacement needed
	if replacementName == "" {
		return query, nil
	}

	visitor := model.NewBaseVisitor()
	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {

		// full text field should be used only in where clause
		if e.ColumnName == model.TimestampFieldName {
			return model.NewColumnRefWithTable(replacementName, e.TableAlias)
		}
		return e
	}
	expr := query.SelectCommand.Accept(visitor)

	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil

}

func (s *SchemaCheckPass) applyFieldEncoding(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {
	table, ok := s.tableDiscovery.TableDefinitions().Load(query.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", query.TableName)
	}
	_, hasAttributesValuesColumn := table.Cols[database_common.AttributesValuesColumn]

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {

		// we don't want to resolve our well know technical fields
		if e.ColumnName == model.FullTextFieldNamePlaceHolder || e.ColumnName == common_table.IndexNameColumn {
			return e
		}

		// This is workaround.
		// Our query parse resolves columns sometimes. Here we detect it and skip the resolution.
		if _, ok := indexSchema.ResolveFieldByInternalName(e.ColumnName); ok {
			logger.Debug().Msgf("Got field already resolved %s", e.ColumnName) // Reduced to debug as it was really noisy
			return e
		}

		if resolvedField, ok := indexSchema.ResolveField(e.ColumnName); ok {
			return model.NewColumnRefWithTable(resolvedField.InternalPropertyName.AsString(), e.TableAlias)
		} else {
			// here we didn't find a column by field name,
			// maybe we should use attributes

			if hasAttributesValuesColumn {
				return model.NewArrayAccess(model.NewColumnRef(database_common.AttributesValuesColumn), model.NewLiteral(fmt.Sprintf("'%s'", e.ColumnName)))
			} else {
				return model.NullExpr
			}
		}
	}

	visitor.OverrideVisitSelectCommand = func(v *model.BaseExprVisitor, query model.SelectCommand) interface{} {
		var columns, groupBy []model.Expr
		var orderBy []model.OrderByExpr
		from := query.FromClause
		where := query.WhereClause

		for _, expr := range query.Columns {
			var alias string
			if e, ok := expr.(model.ColumnRef); ok {
				alias = e.ColumnName
			}

			col := expr.Accept(v).(model.Expr)

			if e, ok := col.(model.ArrayAccess); ok && alias != "" {
				col = model.NewAliasedExpr(e, alias)
			} else if e, ok := col.(model.LiteralExpr); ok && alias != "" && e.Value == "NULL" {
				col = model.NewAliasedExpr(e, alias)
			}

			columns = append(columns, col)
		}
		for _, expr := range query.GroupBy {
			groupBy = append(groupBy, expr.Accept(v).(model.Expr))
		}
		for _, expr := range query.OrderBy {
			orderBy = append(orderBy, expr.Accept(v).(model.OrderByExpr))
		}
		if query.FromClause != nil {
			from = query.FromClause.Accept(v).(model.Expr)
		}
		if query.WhereClause != nil {
			where = query.WhereClause.Accept(v).(model.Expr)
		}

		var namedCTEs []*model.CTE
		if query.NamedCTEs != nil {
			for _, cte := range query.NamedCTEs {
				namedCTEs = append(namedCTEs, cte.Accept(v).(*model.CTE))
			}
		}

		var limitBy []model.Expr
		if query.LimitBy != nil {
			for _, expr := range query.LimitBy {
				limitBy = append(limitBy, expr.Accept(v).(model.Expr))
			}
		}
		return model.NewSelectCommand(columns, groupBy, orderBy, from, where, limitBy, query.Limit, query.SampleLimit, query.IsDistinct, namedCTEs)
	}

	expr := query.SelectCommand.Accept(visitor)

	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}

	return query, nil
}

func (s *SchemaCheckPass) applyRuntimeMappings(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {

	if query.RuntimeMappings == nil {
		return query, nil
	}

	cols := query.SelectCommand.Columns

	// replace column refs with runtime mappings with proper name
	for i, col := range cols {
		switch c := col.(type) {
		case model.ColumnRef:
			if mapping, ok := query.RuntimeMappings[c.ColumnName]; ok {
				cols[i] = model.NewAliasedExpr(mapping.DatabaseExpression, c.ColumnName)
			}
		}
	}
	query.SelectCommand.Columns = cols

	// replace other places where column refs are used
	visitor := model.NewBaseVisitor()
	visitor.OverrideVisitColumnRef = func(b *model.BaseExprVisitor, e model.ColumnRef) interface{} {
		if mapping, ok := query.RuntimeMappings[e.ColumnName]; ok {
			return mapping.DatabaseExpression
		}
		return e
	}

	expr := query.SelectCommand.Accept(visitor)

	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}

// it convers out internal date time related fuction to clickhouse functions
func (s *SchemaCheckPass) convertQueryDateTimeFunctionToClickhouse(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitFunction = func(b *model.BaseExprVisitor, e model.FunctionExpr) interface{} {

		switch e.Name {

		case model.DateHourFunction:
			if len(e.Args) != 1 {
				return e
			}
			return model.NewFunction("toHour", e.Args[0].Accept(b).(model.Expr))

			// TODO this is a place for over date/time related functions
			// add more

		default:
			return visitFunction(b, e)
		}
	}

	expr := query.SelectCommand.Accept(visitor)

	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil

}

func (s *SchemaCheckPass) checkAggOverUnsupportedType(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {

	aggFunctionPrefixes := []string{"sum", "avg", "quantiles"}

	dbTypePrefixes := []string{"DateTime", "String", "LowCardinality(String)"}

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitFunction = func(b *model.BaseExprVisitor, e model.FunctionExpr) interface{} {

		currentFunctionName := strings.ToLower(e.Name)

		for _, aggPrefix := range aggFunctionPrefixes {
			if strings.HasPrefix(currentFunctionName, aggPrefix) {
				if len(e.Args) > 0 {
					if columnRef, ok := e.Args[0].(model.ColumnRef); ok {
						if col, ok := indexSchema.ResolveFieldByInternalName(columnRef.ColumnName); ok {
							for _, dbTypePrefix := range dbTypePrefixes {
								if strings.HasPrefix(col.InternalPropertyType, dbTypePrefix) {
									logger.Warn().Msgf("Aggregation '%s' over unsupported type '%s' in column '%s'", e.Name, dbTypePrefix, col.InternalPropertyName.AsString())
									args := b.VisitChildren(e.Args)
									args[0] = model.NullExpr
									return model.NewFunction(e.Name, args...)
								}
							}
						}
					}
					// attributes values are always string,
					if access, ok := e.Args[0].(model.ArrayAccess); ok {
						if access.ColumnRef.ColumnName == database_common.AttributesValuesColumn {
							logger.Warn().Msgf("Unsupported case. Aggregation '%s' over attribute named: '%s'", e.Name, access.Index)
							args := b.VisitChildren(e.Args)
							args[0] = model.NullExpr
							return model.NewFunction(e.Name, args...)
						}
					}
				}
			}
		}

		return visitFunction(b, e)
	}

	expr := query.SelectCommand.Accept(visitor)

	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil

}

func columnsToAliasedColumns(columns []model.Expr) []model.Expr {
	aliasedColumns := make([]model.Expr, len(columns))
	for i, column := range columns {
		if columnRef, ok := column.(model.ColumnRef); ok {
			aliasedColumns[i] = columnRef
			continue
		}
		if col, ok := column.(model.LiteralExpr); ok {
			if _, isStr := col.Value.(string); !isStr {
				aliasedColumns[i] = model.NewAliasedExpr(column, fmt.Sprintf("column_%d", i))
			} else {
				aliasedColumns[i] = col
			}
			continue
		}
		if aliasedExpr, ok := column.(model.AliasedExpr); ok {
			aliasedColumns[i] = aliasedExpr
			continue
		}
		if _, ok := column.(model.FunctionExpr); ok {
			aliasedColumns[i] = model.NewAliasedExpr(column, fmt.Sprintf("column_%d", i))
			continue
		}
		if _, ok := column.(model.WindowFunction); ok {
			aliasedColumns[i] = model.NewAliasedExpr(column, fmt.Sprintf("column_%d", i))
			continue
		}

		aliasedColumns[i] = model.NewAliasedExpr(column, fmt.Sprintf("column_%d", i))
		logger.Error().Msgf("Quesma internal error - unreachable code: unsupported column type %T", column)
	}
	return aliasedColumns
}

func (s *SchemaCheckPass) applyAliasColumns(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {
	query.SelectCommand.Columns = columnsToAliasedColumns(query.SelectCommand.Columns)
	return query, nil
}

func visitFunction(b *model.BaseExprVisitor, f model.FunctionExpr) interface{} {
	return model.NewFunction(f.Name, b.VisitChildren(f.Args)...)
}

func visitInfix(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
	return model.NewInfixExpr(e.Left.Accept(b).(model.Expr), e.Op, e.Right.Accept(b).(model.Expr))
}

func (s *SchemaCheckPass) acceptIntsAsTimestamps(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {
	table, exists := s.tableDiscovery.TableDefinitions().Load(query.TableName)
	if !exists {
		return nil, fmt.Errorf("table %s not found", query.TableName)
	}

	dateManager := elastic_query_dsl.NewDateManager(context.Background())
	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		col, okLeft := model.ExtractColRef(e.Left)
		lit, _ := model.ToLiteral(e.Right)
		ts, okRight := model.ToLiteralsValue(e.Right)
		if okLeft && okRight && table.IsInt(col.ColumnName) {
			format := ""
			if f, ok := lit.Format(); ok {
				format = f
			}
			expr, ok := dateManager.ParseDateUsualFormat(ts, database_common.DateTime64, format)
			if !ok {
				// FIXME hacky but seems working
				if tsStr, ok_ := ts.(string); ok_ && len(tsStr) > 2 {
					expr, ok = dateManager.ParseDateUsualFormat(tsStr[1:len(tsStr)-1], database_common.DateTime64, format)
				}
			}
			if ok {
				if f, okF := model.ToFunction(expr); okF && f.Name == "fromUnixTimestamp64Milli" && len(f.Args) == 1 {
					if l, okL := model.ToLiteral(f.Args[0]); okL {
						if _, exists := l.Format(); exists { // heuristics: it's a date <=> it has a format
							return model.NewInfixExpr(col, e.Op, f.Args[0])
						}
					}
				}
			}
		}
		return visitInfix(b, e)
	}

	visitor.OverrideVisitFunction = func(b *model.BaseExprVisitor, f model.FunctionExpr) interface{} {
		if f.Name == "toUnixTimestamp64Milli" && len(f.Args) == 1 {
			if col, ok := model.ExtractColRef(f.Args[0]); ok && table.IsInt(col.ColumnName) {
				// erases toUnixTimestamp64Milli
				return f.Args[0]
			}
		}
		if f.Name == "toTimezone" && len(f.Args) == 2 {
			if col, ok := model.ExtractColRef(f.Args[0]); ok && table.IsInt(col.ColumnName) {
				// adds fromUnixTimestamp64Milli
				return model.NewFunction("toTimezone", model.NewFunction("fromUnixTimestamp64Milli", f.Args[0]), f.Args[1])
			}
		}
		return visitFunction(b, f)
	}

	expr := query.SelectCommand.Accept(visitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}

	return query, nil
}

func (s *SchemaCheckPass) Transform(plan *model.ExecutionPlan) (*model.ExecutionPlan, error) {

	transformationChain := []struct {
		TransformationName string
		Transformation     func(schema.Schema, *model.Query) (*model.Query, error)
	}{
		// Section 1: from logical to physical
		{TransformationName: "PhysicalFromExpressionTransformation", Transformation: s.applyPhysicalFromExpression},
		{TransformationName: "WildcardExpansion", Transformation: s.applyWildcardExpansion},
		{TransformationName: "RuntimeMappings", Transformation: s.applyRuntimeMappings},
		{TransformationName: "AllNecessaryCommonTransformations", Transformation: func(schema schema.Schema, query *model.Query) (*model.Query, error) {
			return transformations.ApplyAllNecessaryCommonTransformations(query, schema, s.cfg.MapFieldsDiscoveringEnabled)
		}},
		{TransformationName: "AliasColumnsTransformation", Transformation: s.applyAliasColumns},
		{TransformationName: "AcceptIntsAsTimestamps", Transformation: s.acceptIntsAsTimestamps},

		// Section 2: generic schema based transformations
		//
		// FieldEncodingTransformation should be after WildcardExpansion
		// because WildcardExpansion expands the wildcard to all fields
		// and columns are expanded as PublicFieldName, so we need to encode them
		// or in other words use internal field names
		{TransformationName: "FieldEncodingTransformation", Transformation: s.applyFieldEncoding},
		{TransformationName: "FullTextFieldTransformation", Transformation: s.applyFullTextField},
		{TransformationName: "TimestampFieldTransformation", Transformation: s.applyTimestampField},
		{TransformationName: "ApplySearchAfterParameter", Transformation: s.applySearchAfterParameter},

		// Section 3: clickhouse specific transformations
		{TransformationName: "QuesmaDateFunctions", Transformation: s.convertQueryDateTimeFunctionToClickhouse},
		{TransformationName: "IpTransformation", Transformation: s.applyIpTransformations},
		{TransformationName: "GeoTransformation", Transformation: s.applyGeoTransformations},
		{TransformationName: "ArrayTransformation", Transformation: s.applyArrayTransformations},
		{TransformationName: "MapTransformation", Transformation: s.applyMapTransformations},
		{TransformationName: "MatchOperatorTransformation", Transformation: s.applyMatchOperator},
		{TransformationName: "AggOverUnsupportedType", Transformation: s.checkAggOverUnsupportedType},
		{TransformationName: "ClusterFunction", Transformation: s.applyFromClusterExpression},

		// Section 4: compensations and checks
		{TransformationName: "BooleanLiteralTransformation", Transformation: s.applyBooleanLiteralLowering},
		// Section 5 : FROM CLUSTER if distributed table
		{TransformationName: "ApplySelectFromCluster", Transformation: s.ApplySelectFromCluster},
	}

	for k, query := range plan.Queries {
		var err error

		if !s.cfg.Logging.EnableSQLTracing {
			query.TransformationHistory.SchemaTransformers = append(query.TransformationHistory.SchemaTransformers, "n/a")
		}

		for _, transformation := range transformationChain {

			var inputQuery string

			if s.cfg.Logging.EnableSQLTracing {
				inputQuery = query.SelectCommand.String()
			}

			query, err = transformation.Transformation(query.Schema, query)
			if err != nil {
				return nil, fmt.Errorf("error applying transformation %s: %w", transformation.TransformationName, err)
			}

			if s.cfg.Logging.EnableSQLTracing {
				if query.SelectCommand.String() != inputQuery {
					query.TransformationHistory.SchemaTransformers = append(query.TransformationHistory.SchemaTransformers, transformation.TransformationName)
					logger.Info().Msgf(transformation.TransformationName+" triggered, input query: %s", inputQuery)
					logger.Info().Msgf(transformation.TransformationName+" triggered, output query: %s", query.SelectCommand.String())
				}
			}
		}

		plan.Queries[k] = query
	}
	return plan, nil
}

func (s *SchemaCheckPass) applyMatchOperator(indexSchema schema.Schema, query *model.Query) (*model.Query, error) {

	visitor := model.NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		var (
			lhs                                       = e.Left
			rhs, okRight                              = e.Right.(model.LiteralExpr)
			lhsCol                                    model.ColumnRef
			okLeft, lhsIsArrayAccess, colIsAttributes bool
		)

		// try to extract column from lhs
		switch lhsT := lhs.(type) {
		case model.ColumnRef:
			lhsCol = lhsT
			okLeft = true
		case model.FunctionExpr:
			if len(lhsT.Args) >= 1 {
				if col, ok := lhsT.Args[0].(model.ColumnRef); ok {
					lhsCol = col
					okLeft = true
				} else if f2, ok := lhsT.Args[0].(model.FunctionExpr); ok && len(f2.Args) == 1 {
					if col, ok := f2.Args[0].(model.ColumnRef); ok {
						lhsCol = col
						okLeft = true
					}
				}
			}
		case model.ArrayAccess:
			lhsIsArrayAccess = true
			okLeft = true
			lhsCol = lhsT.ColumnRef
		default:
			return visitInfix(b, e)
		}

		rhsValue, ok := rhs.Value.(string)
		if !ok {
			if e.Op == model.MatchOperator {
				// only strings can be ILIKEd, everything else is a simple =
				return model.NewInfixExpr(e.Left.Accept(b).(model.Expr), "=", e.Right.Accept(b).(model.Expr))
			} else {
				return visitInfix(b, e)
			}
		}

		if okLeft && okRight && e.Op == model.MatchOperator {
			field, found := indexSchema.ResolveFieldByInternalName(lhsCol.ColumnName)
			if !found {
				// indexSchema won't find attributes columns, that's why this check
				if database_common.IsColumnAttributes(lhsCol.ColumnName) {
					colIsAttributes = true
				} else {
					logger.Error().Msgf("Field %s not found in schema for table %s, should never happen here", lhsCol.ColumnName, query.TableName)
					goto experimental
				}
			}

			rhsValue = strings.TrimPrefix(rhsValue, "'")
			rhsValue = strings.TrimSuffix(rhsValue, "'")

			ilike := func() model.Expr {
				return model.NewInfixExpr(lhs, "ILIKE", rhs.Clone())
			}
			equal := func() model.Expr {
				return model.NewInfixExpr(lhs, "=", rhs.Clone())
			}

			// handling case when e.Left is an array access
			if lhsIsArrayAccess {
				if colIsAttributes || field.IsMapWithStringValues() { // attributes always have string values, so ilike
					return ilike()
				} else {
					return equal()
				}
			}

			// handling case when e.Left is a simple column ref
			// TODO: improve? we seem to be `ilike'ing` too much
			switch field.Type.String() {
			case schema.QuesmaTypeInteger.Name, schema.QuesmaTypeLong.Name, schema.QuesmaTypeUnsignedLong.Name, schema.QuesmaTypeFloat.Name, schema.QuesmaTypeBoolean.Name:
				rhs.Value = strings.Trim(rhsValue, "%")
				rhs.Attrs[model.EscapeKey] = model.NormalNotEscaped
				return equal()
			case schema.QuesmaTypeKeyword.Name: // similar as above, but for keyword type, un-ilike it
				rhs.Value = strings.Trim(rhsValue, "%")
				rhs.Attrs[model.EscapeKey] = model.FullyEscaped
				return equal()
			default:
				if rhsValue == "%%" { // ILIKE '%%' has terrible performance, but semantically means "is not null", hence this transformation
					return model.NewInfixExpr(lhs, "IS", model.NewLiteral("NOT NULL"))
				}
				// we might investigate the potential performance gain of checking
				// that if rhsValue doesn't contain '%' we could use '=' instead of 'ILIKE'
				// *however* that'd require few tweaks in the parser
				return ilike()
			}
		}

	experimental:
		if s.isFieldMapSyntaxEnabled(query) {
			// special case where left side is arrayElement,
			// arrayElement comes from applyFieldEncoding function
			arrayElementFn, ok := e.Left.(model.FunctionExpr)
			if ok && arrayElementFn.Name == "arrayElement" && e.Op == model.MatchOperator {

				if len(arrayElementFn.Args) == 2 {
					if col, ok := arrayElementFn.Args[0].(model.ColumnRef); ok {
						field, found := indexSchema.ResolveFieldByInternalName(col.ColumnName)

						if found {
							internalType := field.InternalPropertyType

							// we support Map(K,V) type only
							if strings.HasPrefix(internalType, "Map(") {
								types := strings.TrimPrefix(strings.TrimSuffix(internalType, ")"), "Map(")
								types = strings.ReplaceAll(types, " ", "")
								kvTypes := strings.Split(types, ",")

								// sanity check for map type with two elements
								if len(kvTypes) == 2 {
									rhsValue = rhs.Value.(string)
									rhsValue = strings.TrimPrefix(rhsValue, "'")
									rhsValue = strings.TrimSuffix(rhsValue, "'")

									// here we check if the value of the map is string or not

									if strings.Contains(kvTypes[1], "String") {
										newRhs := rhs.Clone()
										newRhs.Value = rhsValue
										newRhs.Attrs[model.EscapeKey] = model.NotEscapedLikeFull
										return model.NewInfixExpr(arrayElementFn.Accept(b).(model.Expr), "iLIKE", newRhs)
									} else {
										return model.NewInfixExpr(arrayElementFn.Accept(b).(model.Expr), "=", e.Right.Accept(b).(model.Expr))
									}
								}
							}
						}
					}
				}
			}
		}

		if e.Op == model.MatchOperator {
			logger.Error().Msgf("Match operator is not supported for column %v (expr: %v)", lhsCol, e)
		}
		return visitInfix(b, e)
	}

	expr := query.SelectCommand.Accept(visitor)

	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil

}

// applyFromClusterExpression transforms query so that `FROM table` becomes `FROM cluster(clusterName,table)` if applicable
func (s *SchemaCheckPass) applyFromClusterExpression(currentSchema schema.Schema, query *model.Query) (*model.Query, error) {
	if s.cfg.ClusterName == "" {
		return query, nil
	}
	visitor := model.NewBaseVisitor()
	table, ok := s.tableDiscovery.TableDefinitions().Load(query.TableName)
	if !ok {
		return nil, fmt.Errorf("table %s not found", query.TableName)
	}
	if !table.ExistsOnAllNodes {
		return query, nil
	}
	visitor.OverrideVisitTableRef = func(b *model.BaseExprVisitor, e model.TableRef) interface{} {
		return model.NewFunction("cluster", model.NewLiteral(s.cfg.ClusterName), e)
	}
	logger.Debug().Msgf("applyClusterFunction: %s", s.cfg.ClusterName)
	expr := query.SelectCommand.Accept(visitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}
