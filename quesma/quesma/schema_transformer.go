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

type BoolLiteralVisitor struct {
	model.ExprVisitor
}

func (v *BoolLiteralVisitor) VisitLiteral(e model.LiteralExpr) interface{} {
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

func (v *BoolLiteralVisitor) VisitInfix(e model.InfixExpr) interface{} {
	return model.NewInfixExpr(e.Left.Accept(v).(model.Expr), e.Op, e.Right.Accept(v).(model.Expr))
}

func (v *BoolLiteralVisitor) VisitSelectCommand(e model.SelectCommand) interface{} {
	var whereClause model.Expr
	if e.WhereClause != nil {
		whereClause = e.WhereClause.Accept(v).(model.Expr)
	}
	var fromClause model.Expr
	if e.FromClause != nil {
		fromClause = e.FromClause.Accept(v).(model.Expr)
	}

	return model.NewSelectCommand(e.Columns, e.GroupBy, e.OrderBy,
		fromClause, whereClause, e.Limit, e.SampleLimit, e.IsDistinct)
}

func (s *SchemaCheckPass) applyBooleanLiteralLowering(query *model.Query) (*model.Query, error) {
	whereVisitor := &BoolLiteralVisitor{ExprVisitor: model.NoOpVisitor{}}

	expr := query.SelectCommand.Accept(whereVisitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}

type WhereVisitor struct {
	model.ExprVisitor
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
func (v *WhereVisitor) VisitSelectCommand(e model.SelectCommand) interface{} {
	var whereClause model.Expr
	if e.WhereClause != nil {
		whereClause = e.WhereClause.Accept(v).(model.Expr)
	}
	var fromClause model.Expr
	if e.FromClause != nil {
		fromClause = e.FromClause.Accept(v).(model.Expr)
	}

	return model.NewSelectCommand(e.Columns, e.GroupBy, e.OrderBy,
		fromClause, whereClause, e.Limit, e.SampleLimit, e.IsDistinct)
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
	whereVisitor := &WhereVisitor{ExprVisitor: model.NoOpVisitor{}, tableName: fromTable, cfg: s.cfg}

	expr := query.SelectCommand.Accept(whereVisitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}

type GeoIpVisitor struct {
	model.ExprVisitor
	tableName      string
	schemaRegistry schema.Registry
}

func (v *GeoIpVisitor) VisitTableRef(e model.TableRef) interface{} {
	return model.NewTableRef(e.Name)
}

func (v *GeoIpVisitor) VisitSelectCommand(e model.SelectCommand) interface{} {
	if v.schemaRegistry == nil {
		logger.Error().Msg("Schema registry is not set")
		return e
	}
	schemaInstance, exists := v.schemaRegistry.FindSchema(schema.TableName(v.tableName))
	if !exists {
		logger.Error().Msgf("Schema fot table %s not found", v.tableName)
		return e
	}
	var groupBy []model.Expr
	for _, expr := range e.GroupBy {
		groupByExpr := expr.Accept(v).(model.Expr)
		if col, ok := expr.(model.ColumnRef); ok {
			// This checks if the column is of type point
			// and if it is, it appends the lat and lon columns to the group by clause
			if schemaInstance.Fields[schema.FieldName(col.ColumnName)].Type.Name == schema.TypePoint.Name {
				// TODO suffixes ::lat, ::lon are hardcoded for now
				groupBy = append(groupBy, model.NewColumnRef(col.ColumnName+"::lat"))
				groupBy = append(groupBy, model.NewColumnRef(col.ColumnName+"::lon"))
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
			if schemaInstance.Fields[schema.FieldName(col.ColumnName)].Type.Name == schema.TypePoint.Name {
				// TODO suffixes ::lat, ::lon are hardcoded for now
				columns = append(columns, model.NewColumnRef(col.ColumnName+"::lat"))
				columns = append(columns, model.NewColumnRef(col.ColumnName+"::lon"))
			} else {
				columns = append(columns, expr.Accept(v).(model.Expr))
			}
		} else {
			columns = append(columns, expr.Accept(v).(model.Expr))
		}
	}

	var fromClause model.Expr
	if e.FromClause != nil {
		fromClause = e.FromClause.Accept(v).(model.Expr)
	}

	return model.NewSelectCommand(columns, groupBy, e.OrderBy,
		fromClause, e.WhereClause, e.Limit, e.SampleLimit, e.IsDistinct)
}

func (s *SchemaCheckPass) applyGeoTransformations(query *model.Query) (*model.Query, error) {
	fromTable := getFromTable(query.TableName)

	geoIpVisitor := &GeoIpVisitor{ExprVisitor: model.NoOpVisitor{}, tableName: fromTable, schemaRegistry: s.schemaRegistry}
	expr := query.SelectCommand.Accept(geoIpVisitor)
	if _, ok := expr.(*model.SelectCommand); ok {
		query.SelectCommand = *expr.(*model.SelectCommand)
	}
	return query, nil
}

func (s *SchemaCheckPass) applyArrayTransformations(query *model.Query) (*model.Query, error) {
	fromTable := getFromTable(query.TableName)

	visitor := &ArrayTypeVisitor{tableName: fromTable, schemaRegistry: s.schemaRegistry, logManager: s.logManager}
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
			{TransformationName: "BooleanLiteralTransformation", Transformation: s.applyBooleanLiteralLowering},
			{TransformationName: "IpTransformation", Transformation: s.applyIpTransformations},
			{TransformationName: "GeoTransformation", Transformation: s.applyGeoTransformations},
			{TransformationName: "ArrayTransformation", Transformation: s.applyArrayTransformations},
		}
		for _, transformation := range transformationChain {
			inputQuery := query.SelectCommand.String()
			query, err = transformation.Transformation(query)
			if query.SelectCommand.String() != inputQuery {
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
