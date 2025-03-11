package frontend_connectors

/*
func Test_validateAndParse(t *testing.T) {
	fields := map[schema.FieldName]schema.Field{
		"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
		"@timestamp": {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeDate},
	}
	Schema := schema.NewSchema(fields, true, "", nil) // TODO nil?

	var testcases = []struct {
		searchAfter                     any
		isInputFineBulletproofStrategy  bool
		isInputFineBasicAndFastStrategy bool
	}{
		{nil, true, true},
		{[]any{}, false, false},
		{[]any{1}, true, true},
		{[]any{1.0}, true, true},
		{[]any{1.1}, false, false},
		{[]any{-1}, false, false},
		{[]any{1, "abc"}, true, true}, // true because we add an additional order by column
		{"string is bad", false, false},
		{[]any{10, 20, 30, 40}, true, false},
	}

	strategies := []model.SearchAfterStrategy{
		SearchAfterStrategyFactory(model.BasicAndFast),
		SearchAfterStrategyFactory(model.Bulletproof),
	}
	for _, strategy := range strategies {
		for i, tc := range testcases {
			t.Run(fmt.Sprintf("%v (testNr:%d)", tc.searchAfter, i), func(t *testing.T) {
				query := &model.Query{}
				query.SelectCommand.OrderBy = []model.OrderByExpr{model.NewOrderByExprWithoutOrder(model.NewColumnRef("@timestamp"))}
				if arr, ok := tc.searchAfter.([]any); ok && len(arr) == 2 {
					query.SelectCommand.OrderBy = append(query.SelectCommand.OrderBy, model.NewOrderByExprWithoutOrder(model.NewColumnRef("message")))
				}
				query.SearchAfter = tc.searchAfter
				err := strategy.ValidateAndParse(query, Schema)

				if _, ok := strategy.(*searchAfterStrategyBulletproof); ok && (err == nil) != tc.isInputFineBulletproofStrategy {
					t.Errorf("Bulletproof strategy failed to validate the input: %v, err: %v", tc.searchAfter, err)
				}
				if _, ok := strategy.(*searchAfterStrategyBasicAndFast); ok && (err == nil) != tc.isInputFineBasicAndFastStrategy {
					t.Errorf("BasicAndFast strategy failed to validate the input: %v, err: %v", tc.searchAfter, err)
				}
			})
		}
	}
}

func Test_applySearchAfterParameter(t *testing.T) {
	t.Skip("napraw to")
	fields := map[schema.FieldName]schema.Field{
		"message":    {PropertyName: "message", InternalPropertyName: "message", Type: schema.QuesmaTypeText},
		"@timestamp": {PropertyName: "@timestamp", InternalPropertyName: "@timestamp", Type: schema.QuesmaTypeDate},
	}
	Schema := schema.NewSchema(fields, true, "", nil) // TODO nil?

	indexConfig := map[string]config.IndexConfiguration{"kibana_sample_data_ecommerce": {}}

	tableMap := clickhouse.NewTableMap()
	tableDiscovery := clickhouse.NewEmptyTableDiscovery()
	tableDiscovery.TableMap = tableMap
	for indexName := range indexConfig {
		tableMap.Store(indexName, clickhouse.NewEmptyTable(indexName))
	}

	singleOrderBy := []model.OrderByExpr{model.NewOrderByExpr(model.NewColumnRef("@timestamp"), model.DescOrder)}
	selectJustOrderBy := model.SelectCommand{OrderBy: singleOrderBy}
	emptyQuery := func() *model.Query { return &model.Query{SelectCommand: selectJustOrderBy} }
	withWhere := func(query *model.Query, timestamp any) *model.Query {
		additionalWhere := model.NewInfixExpr(model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(1)), ">", model.NewColumnRef("@timestamp"))
		query.SelectCommand.WhereClause = model.And([]model.Expr{query.SelectCommand.WhereClause, additionalWhere})
		return query
	}
	oneRealQuery := func() *model.Query {
		return &model.Query{
			TableName: "kibana_sample_data_logs",
			SelectCommand: model.SelectCommand{
				FromClause: model.NewTableRef("kibana_sample_data_logs"),
				Columns:    []model.Expr{model.NewColumnRef("message")},
				OrderBy:    singleOrderBy,
				WhereClause: &model.InfixExpr{
					Left: &model.InfixExpr{
						Left: &model.InfixExpr{
							Left: &model.LiteralExpr{Value: strconv.Quote("@timestamp")},
							Op:   ">=",
							Right: &model.FunctionExpr{
								Name: "parseDateTime64BestEffort",
								Args: []model.Expr{&model.LiteralExpr{Value: "'2024-06-06T09:58:50.387Z'"}}},
						},
						Op: "AND",
						Right: &model.InfixExpr{
							Left: &model.LiteralExpr{Value: strconv.Quote("@timestamp")},
							Op:   "<=",
							Right: &model.FunctionExpr{
								Name: "parseDateTime64BestEffort",
								Args: []model.Expr{&model.LiteralExpr{Value: "'2024-06-10T09:58:50.387Z'"}}},
						},
					},
					Op: "AND",
					Right: &model.FunctionExpr{
						Name: "a",
						Args: []model.Expr{
							&model.FunctionExpr{
								Name: "b",
								Args: []model.Expr{
									&model.AliasedExpr{
										Expr: &model.FunctionExpr{
											Name: "c",
											Args: []model.Expr{
												&model.LiteralExpr{Value: 8},
												&model.LiteralExpr{Value: "'0.0.0.0'"},
											},
										},
										Alias: "happy alias",
									},
								},
							},
							&model.LiteralExpr{Value: "happy literal"},
						},
					},
				},
			},
		}
	}
	_ = withWhere
	_ = oneRealQuery

	var testcases = []struct {
		searchAfter              any
		query                    *model.Query
		transformedQueryExpected *model.Query
		errorExpected            bool
	}{
		//{nil, emptyQuery(), emptyQuery(), false},
		{[]any{}, emptyQuery(), emptyQuery(), true},
		//{[]any{1}, emptyQuery(), withWhere(emptyQuery(), 1), false},
		//{[]any{1.0}, emptyQuery(), withWhere(emptyQuery(), 1), false},
		//{[]any{1.1}, emptyQuery(), emptyQuery(), true},
		//{[]any{5, 10}, emptyQuery(), emptyQuery(), true},
		//{[]any{-1}, emptyQuery(), emptyQuery(), true},
		//{"string is bad", emptyQuery(), emptyQuery(), true},
		//{[]any{int64(1)}, oneRealQuery(), withWhere(oneRealQuery(), 1), false},
	}

	strategies := []model.SearchAfterStrategyType{model.BasicAndFast}
	for _, strategy := range strategies {
		for i, tc := range testcases {
			t.Run(fmt.Sprintf("%v (testNr:%d)", tc.searchAfter, i), func(t *testing.T) {
				// apply search_after parameter, easier to do here than in all the testcases
				tc.query.SearchAfter = tc.searchAfter
				tc.query.SearchAfterStrategy = SearchAfterStrategyFactory(strategy)
				tc.transformedQueryExpected.SearchAfter = tc.searchAfter

				err := tc.query.SearchAfterStrategy.ValidateAndParse(tc.query, Schema)
				fmt.Println("err validate_and_parse", err)
				assert.Equal(t, tc.errorExpected, err != nil, "Expected error: %v, got: %v", tc.errorExpected, err)
				actual, err := tc.query.SearchAfterStrategy.TransformQuery(tc.query)
				assert.Equal(t, tc.errorExpected, err != nil, "Expected error: %v, got: %v", tc.errorExpected, err)
				if err == nil {
					assert.Equal(t,
						model.AsString(tc.transformedQueryExpected.SelectCommand),
						model.AsString(actual.SelectCommand),
						"Expected:\n%v,\ngot:\n%v", tc.transformedQueryExpected, actual,
					)
				}
			})
		}
	}
}

*/
