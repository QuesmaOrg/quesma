// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package queryparser

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/k0kubun/pp"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/model"
	"quesma/model/bucket_aggregations"
	"quesma/model/typical_queries"
	"quesma/queryparser/lucene"
	"quesma/quesma/types"
	"quesma/schema"
	"quesma/util"
	"strconv"
	"strings"
	"unicode"
)

type QueryMap = map[string]interface{}

// NewEmptyHighlighter returns no-op for error branches and tests
func NewEmptyHighlighter() model.Highlighter {
	return model.Highlighter{
		Tokens: make(map[string]model.Tokens),
	}
}

const (
	defaultQueryResultSize = 10
	defaultTrackTotalHits  = 10000
)

func (cw *ClickhouseQueryTranslator) ParseQuery(body types.JSON) (*model.ExecutionPlan, error) {

	simpleQuery, hitsInfo, highlighter, err := cw.parseQueryInternal(body)
	if err != nil || !simpleQuery.CanParse {
		logger.WarnWithCtx(cw.Ctx).Msgf("error parsing query: %v", err)
		return &model.ExecutionPlan{}, err
	}

	var queries []*model.Query

	// countQuery will be added later, depending on pancake optimization
	countQuery := cw.buildCountQueryIfNeeded(simpleQuery, hitsInfo)

	// here we decide if pancake should count rows
	addCount := countQuery != nil

	if pancakeQueries, err := cw.PancakeParseAggregationJson(body, addCount); err == nil {
		if len(pancakeQueries) > 0 {
			countQuery = nil // count was taken care of by pancake
		}
		queries = append(queries, pancakeQueries...)
	} else {
		// Currently we swallow error to preserve backward compatibility, but eventually we should return it.
		logger.ErrorWithCtx(cw.Ctx).Msgf("Error parsing pancake queries: %v.", err)
	}

	if countQuery != nil {
		queries = append(queries, countQuery)
	}

	if listQuery := cw.buildListQueryIfNeeded(simpleQuery, hitsInfo, highlighter); listQuery != nil {
		queries = append(queries, listQuery)
	}

	runtimeMappings, err := ParseRuntimeMappings(body) // we apply post query transformer for certain aggregation types
	if err != nil {
		return &model.ExecutionPlan{}, err
	}

	// we apply post query transformer for certain aggregation types
	// this should be a part of the query parsing process

	queryResultTransformers := make([]model.QueryRowsTransformer, len(queries))
	for i, query := range queries {
		switch agg := query.Type.(type) {
		case *bucket_aggregations.Histogram:
			queryResultTransformers[i] = agg.NewRowsTransformer()

		case *bucket_aggregations.DateHistogram:
			queryResultTransformers[i] = agg.NewRowsTransformer()
		}
	}

	for _, query := range queries {
		query.TableName = cw.Table.Name
		query.RuntimeMappings = runtimeMappings
		query.Indexes = cw.Indexes
		query.Schema = cw.Schema
		query.AlternativeSelectCommand = buildAlternativeSelectCommand(&query.SelectCommand)
	}

	plan := &model.ExecutionPlan{
		Queries:               queries,
		QueryRowsTransformers: queryResultTransformers,
	}

	return plan, err
}

const timeRangeOptimization = 48 * 60 * 60 * 1000 // 48 hours

func buildAlternativeSelectCommand(selectCommand *model.SelectCommand) *model.SelectCommand {
	if selectCommand == nil {
		return nil
	}
	if selectCommand.IsDistinct {
		return nil
	}
	for _, column := range selectCommand.Columns {
		if _, isLiteralExpr := column.(model.LiteralExpr); !isLiteralExpr {
			return nil
		}
	}

	var orderByColumnName string

	if len(selectCommand.OrderBy) != 1 {
		return nil
	}
	if orderByColumn, ok := selectCommand.OrderBy[0].Expr.(model.ColumnRef); ok {
		orderByColumnName = orderByColumn.ColumnName
		if selectCommand.OrderBy[0].Direction != model.DescOrder { // TODO: relax
			return nil
		}
	} else {
		return nil
	}

	var upperBound *int64

	visitor := model.NewBaseVisitor()
	visitor.OverrideVisitInfix = func(b *model.BaseExprVisitor, e model.InfixExpr) interface{} {
		if _, ok := e.Left.(model.ColumnRef); !ok {
			e.Left.Accept(b)
			e.Right.Accept(b)
			return e
		}
		if e.Left.(model.ColumnRef).ColumnName != orderByColumnName {
			e.Left.Accept(b)
			e.Right.Accept(b)
			return e
		}
		switch e.Op {
		case "<", "<=":
			if functionExpr, ok := e.Right.(model.FunctionExpr); ok && functionExpr.Name == "fromUnixTimestamp64Milli" {
				upperBoundValue := functionExpr.Args[0].(model.LiteralExpr).Value.(int64)
				upperBound = &upperBoundValue
			}
		}
		e.Left.Accept(b)
		e.Right.Accept(b)
		return e
	}
	selectCommand.Accept(visitor)
	if upperBound == nil {
		return nil
	}
	if selectCommand.GroupBy != nil {
		return nil
	}
	if selectCommand.Limit == 0 {
		return nil
	}
	if len(selectCommand.LimitBy) != 0 {
		return nil
	}
	if selectCommand.SampleLimit != 0 {
		return nil
	}
	if len(selectCommand.NamedCTEs) != 0 {
		return nil
	}
	alternativeSelectCommand := *selectCommand
	alternativeSelectCommand.WhereClause = model.NewInfixExpr(model.NewInfixExpr(model.NewColumnRef(orderByColumnName), ">=", model.NewFunction("fromUnixTimestamp64Milli", model.NewLiteral(*upperBound-timeRangeOptimization))), "AND", selectCommand.WhereClause)
	return &alternativeSelectCommand
}

func (cw *ClickhouseQueryTranslator) buildListQueryIfNeeded(
	simpleQuery *model.SimpleQuery, queryInfo model.HitsCountInfo, highlighter model.Highlighter) *model.Query {
	var fullQuery *model.Query
	switch queryInfo.Typ {
	case model.ListByField:
		// queryInfo = (ListByField, fieldName, 0, LIMIT)
		fullQuery = cw.BuildNRowsQuery(queryInfo.RequestedFields, simpleQuery, queryInfo.Size)
	case model.ListAllFields:
		fullQuery = cw.BuildNRowsQuery([]string{"*"}, simpleQuery, queryInfo.Size)
	default:
	}
	if fullQuery != nil {
		highlighter.SetTokensToHighlight(fullQuery.SelectCommand)
		// TODO: pass right arguments
		queryType := typical_queries.NewHits(cw.Ctx, cw.Table, &highlighter, fullQuery.SelectCommand.OrderByFieldNames(), true, false, false, cw.Indexes)
		fullQuery.Type = &queryType
		fullQuery.Highlighter = highlighter
	}

	return fullQuery
}

func (cw *ClickhouseQueryTranslator) buildCountQueryIfNeeded(simpleQuery *model.SimpleQuery, queryInfo model.HitsCountInfo) *model.Query {
	if queryInfo.TrackTotalHits == model.TrackTotalHitsFalse {
		return nil
	}
	if queryInfo.TrackTotalHits == model.TrackTotalHitsTrue {
		return cw.BuildCountQuery(simpleQuery.WhereClause, 0)
	}
	if queryInfo.TrackTotalHits > queryInfo.Size {
		return cw.BuildCountQuery(simpleQuery.WhereClause, queryInfo.TrackTotalHits)
	}
	return nil
}

func (cw *ClickhouseQueryTranslator) parseQueryInternal(body types.JSON) (*model.SimpleQuery, model.HitsCountInfo, model.Highlighter, error) {
	queryAsMap := body.Clone()

	// we must parse "highlights" here, because it is stripped from the queryAsMap later
	highlighter := cw.ParseHighlighter(queryAsMap)

	var parsedQuery model.SimpleQuery
	if queryPart, ok := queryAsMap["query"]; ok {
		parsedQuery = cw.parseQueryMap(queryPart.(QueryMap))
	} else {
		parsedQuery = model.NewSimpleQuery(nil, true)
	}

	if sortPart, ok := queryAsMap["sort"]; ok {
		parsedQuery.OrderBy = cw.parseSortFields(sortPart)
	}
	size := cw.parseSize(queryAsMap, defaultQueryResultSize)

	trackTotalHits := defaultTrackTotalHits
	if trackTotalHitsRaw, ok := queryAsMap["track_total_hits"]; ok {
		switch trackTotalHitsTyped := trackTotalHitsRaw.(type) {
		case bool:
			if trackTotalHitsTyped {
				trackTotalHits = model.TrackTotalHitsTrue
			} else {
				trackTotalHits = model.TrackTotalHitsFalse
			}
		case float64:
			trackTotalHits = int(trackTotalHitsTyped)
		default:
			logger.WarnWithCtx(cw.Ctx).Msgf("unknown track_total_hits format, track_total_hits value: %v type: %T. Using default (%d)",
				trackTotalHitsRaw, trackTotalHitsRaw, defaultTrackTotalHits)
		}
	}

	queryInfo := cw.tryProcessSearchMetadata(queryAsMap)
	queryInfo.Size = size
	queryInfo.TrackTotalHits = trackTotalHits

	return &parsedQuery, queryInfo, highlighter, nil
}

func (cw *ClickhouseQueryTranslator) ParseHighlighter(queryMap QueryMap) model.Highlighter {

	highlight, ok := queryMap["highlight"].(QueryMap)

	// if the kibana is not interested in highlighting, we return dummy object
	if !ok {
		return NewEmptyHighlighter()
	}

	var highlighter model.Highlighter

	if pre, ok := highlight["pre_tags"]; ok {
		for _, x := range pre.([]interface{}) {
			if xAsString, ok := x.(string); ok {
				highlighter.PreTags = append(highlighter.PreTags, xAsString)
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("unknown pre tag format, pre tag value: %v type: %T. Skipping", x, x)
			}
		}
	}
	if post, ok := highlight["post_tags"]; ok {
		for _, x := range post.([]interface{}) {
			if xAsString, ok := x.(string); ok {
				highlighter.PostTags = append(highlighter.PostTags, xAsString)
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("unknown post tag format, post tag value: %v type: %T. Skipping", x, x)
			}
		}
	}

	// TODO parse other fields:
	// - fields
	// - fragment_size
	return highlighter
}

func (cw *ClickhouseQueryTranslator) ParseQueryAsyncSearch(queryAsJson string) (model.SimpleQuery, model.HitsCountInfo, model.Highlighter) {
	queryAsMap, err := types.ParseJSON(queryAsJson)
	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("error parsing query request's JSON")
		return model.NewSimpleQuery(nil, false), model.NewEmptyHitsCountInfo(), NewEmptyHighlighter()
	}

	// we must parse "highlights" here, because it is stripped from the queryAsMap later
	highlighter := cw.ParseHighlighter(queryAsMap)

	var parsedQuery model.SimpleQuery
	if query, ok := queryAsMap["query"]; ok {
		queryMap, ok := query.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid query type: %T, value: %v", query, query)
			return model.NewSimpleQuery(nil, false), model.NewEmptyHitsCountInfo(), NewEmptyHighlighter()
		}
		parsedQuery = cw.parseQueryMap(queryMap)
	} else {
		return model.NewSimpleQuery(nil, true), cw.tryProcessSearchMetadata(queryAsMap), highlighter
	}

	if sort, ok := queryAsMap["sort"]; ok {
		parsedQuery.OrderBy = cw.parseSortFields(sort)
	}
	queryInfo := cw.tryProcessSearchMetadata(queryAsMap)

	return parsedQuery, queryInfo, highlighter
}

// Metadata attributes are the ones that are on the same level as query tag
// They are moved into separate map for further processing if needed
func (cw *ClickhouseQueryTranslator) parseMetadata(queryMap QueryMap) QueryMap {
	queryMetadata := make(QueryMap, 5)
	for k, v := range queryMap {
		if k == "query" || k == "bool" || k == "query_string" || k == "index_filter" { // probably change that, made so tests work, but let's see after more real use cases {
			continue
		}
		queryMetadata[k] = v
		delete(queryMap, k)
	}
	return queryMetadata
}

func (cw *ClickhouseQueryTranslator) ParseAutocomplete(indexFilter *QueryMap, fieldName string, prefix *string, caseIns bool) model.SimpleQuery {
	fieldName = cw.ResolveField(cw.Ctx, fieldName)
	canParse := true
	stmts := make([]model.Expr, 0)
	if indexFilter != nil {
		res := cw.parseQueryMap(*indexFilter)
		canParse = res.CanParse
		stmts = append(stmts, res.WhereClause)
	}
	if prefix != nil && len(*prefix) > 0 {
		// Maybe quote it?
		var like string
		if caseIns {
			like = "iLIKE"
		} else {
			like = "LIKE"
		}
		stmt := model.NewInfixExpr(model.NewColumnRef(fieldName), like, model.NewLiteral("'"+*prefix+"%'"))
		stmts = append(stmts, stmt)
	}
	return model.NewSimpleQuery(model.And(stmts), canParse)
}

func (cw *ClickhouseQueryTranslator) parseQueryMap(queryMap QueryMap) model.SimpleQuery {
	if len(queryMap) != 1 {
		// TODO suppress metadata for now
		_ = cw.parseMetadata(queryMap)
	}
	parseMap := map[string]func(QueryMap) model.SimpleQuery{
		"match_all":           cw.parseMatchAll,
		"match":               func(qm QueryMap) model.SimpleQuery { return cw.parseMatch(qm, false) },
		"multi_match":         cw.parseMultiMatch,
		"bool":                cw.parseBool,
		"term":                cw.parseTerm,
		"terms":               cw.parseTerms,
		"query":               cw.parseQueryMap,
		"prefix":              cw.parsePrefix,
		"nested":              cw.parseNested,
		"match_phrase":        func(qm QueryMap) model.SimpleQuery { return cw.parseMatch(qm, true) },
		"range":               cw.parseRange,
		"exists":              cw.parseExists,
		"ids":                 cw.parseIds,
		"constant_score":      cw.parseConstantScore,
		"wildcard":            cw.parseWildcard,
		"query_string":        cw.parseQueryString,
		"simple_query_string": cw.parseQueryString,
		"regexp":              cw.parseRegexp,
		"geo_bounding_box":    cw.parseGeoBoundingBox,
	}
	for k, v := range queryMap {
		if f, ok := parseMap[k]; ok {
			if vAsQueryMap, ok := v.(QueryMap); ok {
				return f(vAsQueryMap)
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("query is not a dict. key: %s, value: %v", k, v)
			}
		} else {
			logger.WarnWithCtxAndReason(cw.Ctx, logger.ReasonUnsupportedQuery(k)).Msgf("unsupported query type: %s, value: %v", k, v)
		}
	}
	if len(queryMap) == 0 { // empty query is a valid query
		return model.NewSimpleQuery(nil, true)
	}

	// if we can't parse the query, we should show the bug
	unparsedQuery := pp.Sprint(queryMap)
	if prettyMarshal, err := json.Marshal(queryMap); err == nil {
		unparsedQuery = string(prettyMarshal)
	}
	logger.Error().Msgf("can't parse query: %s", unparsedQuery)
	return model.NewSimpleQuery(nil, false)
}

// `constant_score` query is just a wrapper for filter query which returns constant relevance score, which we ignore anyway
func (cw *ClickhouseQueryTranslator) parseConstantScore(queryMap QueryMap) model.SimpleQuery {
	if _, ok := queryMap["filter"]; ok {
		return cw.parseBool(queryMap)
	} else {
		logger.Error().Msgf("parsing error: `constant_score` needs to wrap `filter` query")
		return model.NewSimpleQuery(nil, false)
	}
}

func (cw *ClickhouseQueryTranslator) parseIds(queryMap QueryMap) model.SimpleQuery {
	var ids, finalIds []string
	if val, ok := queryMap["values"]; ok {
		if values, ok := val.([]interface{}); ok {
			for _, id := range values {
				ids = append(ids, id.(string))
			}
		}
	} else {
		logger.Error().Msgf("parsing error: missing mandatory `values` field")
		return model.NewSimpleQuery(nil, false)
	}
	logger.Warn().Msgf("unsupported id query executed, requested ids of [%s]", strings.Join(ids, "','"))

	timestampColumnName := model.TimestampFieldName

	if len(ids) == 0 {
		logger.Warn().Msgf("parsing error: empty _id array")
		return model.NewSimpleQuery(nil, false)
	}

	// when our generated ID appears in query looks like this: `1d<TRUNCATED>0b8q1`
	// therefore we need to strip the hex part (before `q`) and convert it to decimal
	// then we can query at DB level
	for i, id := range ids {
		idInHex := strings.Split(id, "q")[0]
		if idAsStr, err := hex.DecodeString(idInHex); err != nil {
			logger.Error().Msgf("error parsing document id %s: %v", id, err)
			return model.NewSimpleQuery(nil, true)
		} else {
			tsWithoutTZ := strings.TrimSuffix(string(idAsStr), " +0000 UTC")
			ids[i] = fmt.Sprintf("'%s'", tsWithoutTZ)
		}
	}

	var whereStmt model.Expr
	// TODO replace with cw.Schema
	if v, ok := cw.Table.Cols[timestampColumnName]; ok {
		switch v.Type.String() {
		case clickhouse.DateTime64.String():
			for _, id := range ids {
				finalIds = append(finalIds, fmt.Sprintf("toDateTime64(%s,3)", id))
			}
			if len(finalIds) == 1 {
				whereStmt = model.NewInfixExpr(model.NewColumnRef(timestampColumnName), " = ", model.NewFunction("toDateTime64", model.NewLiteral(ids[0]), model.NewLiteral("3")))
			} else {
				whereStmt = model.NewInfixExpr(model.NewColumnRef(timestampColumnName), " IN ", model.NewFunction("toDateTime64", model.NewLiteral(strings.Join(ids, ",")), model.NewLiteral("3")))
			}
		case clickhouse.DateTime.String():
			for _, id := range ids {
				finalIds = append(finalIds, fmt.Sprintf("toDateTime(%s)", id))
			}
			if len(finalIds) == 1 {
				whereStmt = model.NewInfixExpr(model.NewColumnRef(timestampColumnName), " = ", model.NewFunction("toDateTime", model.NewLiteral(finalIds[0])))
			} else {
				whereStmt = model.NewInfixExpr(model.NewColumnRef(timestampColumnName), " IN ", model.NewFunction("toDateTime", model.NewLiteral(strings.Join(ids, ","))))
			}
		default:
			logger.Warn().Msgf("timestamp field of unsupported type %s", v.Type.String())
			return model.NewSimpleQuery(nil, true)
		}
	}
	return model.NewSimpleQuery(whereStmt, true)
}

// Parses each model.SimpleQuery separately, returns list of translated SQLs
func (cw *ClickhouseQueryTranslator) parseQueryMapArray(queryMaps []interface{}) (stmts []model.Expr, canParse bool) {
	stmts = make([]model.Expr, len(queryMaps))
	canParse = true
	for i, v := range queryMaps {
		if vAsMap, ok := v.(QueryMap); ok {
			query := cw.parseQueryMap(vAsMap)
			stmts[i] = query.WhereClause
			if !query.CanParse {
				canParse = false
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid query type: %T, value: %v", v, v)
			canParse = false
		}
	}
	return stmts, canParse
}

func (cw *ClickhouseQueryTranslator) iterateListOrDictAndParse(queryMaps interface{}) (stmts []model.Expr, canParse bool) {
	switch queryMapsTyped := queryMaps.(type) {
	case []interface{}:
		return cw.parseQueryMapArray(queryMapsTyped)
	case QueryMap:
		simpleQuery := cw.parseQueryMap(queryMapsTyped)
		if simpleQuery.WhereClause != nil {
			return []model.Expr{simpleQuery.WhereClause}, simpleQuery.CanParse
		}
		return []model.Expr{}, simpleQuery.CanParse
	default:
		logger.WarnWithCtx(cw.Ctx).Msgf("Invalid query type: %T, value: %v", queryMapsTyped, queryMapsTyped)
		return []model.Expr{}, false
	}
}

// TODO: minimum_should_match parameter. Now only ints supported and >1 changed into 1
func (cw *ClickhouseQueryTranslator) parseBool(queryMap QueryMap) model.SimpleQuery {
	var andStmts []model.Expr
	canParse := true // will stay true only if all subqueries can be parsed
	for _, andPhrase := range []string{"must", "filter"} {
		if queries, ok := queryMap[andPhrase]; ok {
			newAndStmts, canParseThis := cw.iterateListOrDictAndParse(queries)
			andStmts = append(andStmts, newAndStmts...)
			canParse = canParse && canParseThis
		}
	}
	sql := model.And(andStmts)

	minimumShouldMatch := 0
	if v, ok := queryMap["minimum_should_match"]; ok {
		if vAsFloat, ok := v.(float64); ok {
			minimumShouldMatch = int(vAsFloat)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid minimum_should_match type: %T, value: %v", v, v)
		}
	}
	if len(andStmts) == 0 {
		minimumShouldMatch = 1
	}
	if minimumShouldMatch > 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("minimum_should_match > 1 not supported, changed to 1")
		minimumShouldMatch = 1
	}
	if queries, ok := queryMap["should"]; ok && minimumShouldMatch == 1 {
		orSqls, canParseThis := cw.iterateListOrDictAndParse(queries)
		orSql := model.Or(orSqls)
		canParse = canParse && canParseThis
		if len(andStmts) == 0 {
			sql = orSql
		} else if orSql != nil {
			sql = model.And([]model.Expr{sql, orSql})
		}
	}

	if queries, ok := queryMap["must_not"]; ok {
		sqlNots, canParseThis := cw.iterateListOrDictAndParse(queries)
		canParse = canParse && canParseThis
		if len(sqlNots) > 0 {
			// transform NOT a && NOT b && NOT c --> NOT (a OR b OR c)
			sqlNot := model.NewPrefixExpr("NOT", []model.Expr{model.Or(sqlNots)})
			sql = model.And([]model.Expr{sql, sqlNot})
		}
	}
	return model.NewSimpleQuery(sql, canParse)
}

func (cw *ClickhouseQueryTranslator) parseTerm(queryMap QueryMap) model.SimpleQuery {
	var whereClause model.Expr
	if len(queryMap) == 1 {
		for k, v := range queryMap {
			if k == "_index" { // index is a table name, already taken from URI and moved to FROM clause
				logger.Warn().Msgf("term %s=%v in query body, ignoring in result SQL", k, v)
				whereClause = model.NewInfixExpr(model.NewLiteral("0"), "=", model.NewLiteral("0 /* "+k+"="+sprint(v)+" */"))
				return model.NewSimpleQuery(whereClause, true)
			}
			fieldName := cw.ResolveField(cw.Ctx, k)
			whereClause = model.NewInfixExpr(model.NewColumnRef(fieldName), "=", model.NewLiteral(sprint(v)))
			return model.NewSimpleQuery(whereClause, true)
		}
	}
	logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 term, got: %d. value: %v", len(queryMap), queryMap)
	return model.NewSimpleQuery(nil, false)
}

// TODO remove optional parameters like boost
func (cw *ClickhouseQueryTranslator) parseTerms(queryMap QueryMap) model.SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 term, got: %d. value: %v", len(queryMap), queryMap)
		return model.NewSimpleQuery(nil, false)
	}

	for k, v := range queryMap {
		if strings.HasPrefix(k, "_") {
			// terms enum API uses _tier terms ( data_hot, data_warm, etc.)
			// we don't want these internal fields to percolate to the SQL query
			return model.NewSimpleQuery(nil, true)
		}
		vAsArray, ok := v.([]interface{})
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid terms type: %T, value: %v", v, v)
			return model.NewSimpleQuery(nil, false)
		}
		if len(vAsArray) == 1 {
			simpleStatement := model.NewInfixExpr(model.NewColumnRef(k), "=", model.NewLiteral(sprint(vAsArray[0])))
			return model.NewSimpleQuery(simpleStatement, true)
		}
		values := make([]string, len(vAsArray))
		for i, v := range vAsArray {
			values[i] = sprint(v)
		}
		combinedValues := "(" + strings.Join(values, ",") + ")"
		compoundStatement := model.NewInfixExpr(model.NewColumnRef(k), "IN", model.NewLiteral(combinedValues))
		return model.NewSimpleQuery(compoundStatement, true)
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return model.NewSimpleQuery(nil, false)
}

func (cw *ClickhouseQueryTranslator) parseMatchAll(_ QueryMap) model.SimpleQuery {
	return model.NewSimpleQuery(nil, true)
}

// Supports 'match' and 'match_phrase' queries.
// 'match_phrase' == true -> match_phrase query, else match query
// TODO
// * support optional parameters
// - auto_generate_synonyms_phrase_query
// (Optional, Boolean) If true, match phrase queries are automatically created for multi-term synonyms. Defaults to true.
// - max_expansions
// (Optional, integer) Maximum number of terms to which the query will expand. Defaults to 50.
// - fuzzy_transpositions
// (Optional, Boolean) If true, edits for fuzzy matching include transpositions of two adjacent characters (ab → ba). Defaults to true.
func (cw *ClickhouseQueryTranslator) parseMatch(queryMap QueryMap, matchPhrase bool) model.SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 match, got: %d. value: %v", len(queryMap), queryMap)
		return model.NewSimpleQuery(nil, false)
	}

	for fieldName, v := range queryMap {
		fieldName = cw.ResolveField(cw.Ctx, fieldName)
		// (fieldName, v) = either e.g. ("message", "this is a test")
		//                  or  ("message", map["query": "this is a test", ...]). Here we only care about "query" until we find a case where we need more.
		vUnNested := v
		if vAsQueryMap, ok := v.(QueryMap); ok {
			vUnNested = vAsQueryMap["query"]
		}
		if vAsString, ok := vUnNested.(string); ok {
			var subQueries []string
			if matchPhrase {
				subQueries = []string{vAsString}
			} else {
				subQueries = strings.Split(vAsString, " ")
			}
			statements := make([]model.Expr, 0, len(subQueries))
			for _, subQuery := range subQueries {
				if fieldName == "_id" { // We compute this field on the fly using our custom logic, so we have to parse it differently
					computedIdMatchingQuery := cw.parseIds(QueryMap{"values": []interface{}{subQuery}})
					statements = append(statements, computedIdMatchingQuery.WhereClause)
				} else {
					simpleStat := model.NewInfixExpr(model.NewColumnRef(fieldName), model.MatchOperator, model.NewLiteral("'"+subQuery+"'"))
					statements = append(statements, simpleStat)
				}
			}
			return model.NewSimpleQuery(model.Or(statements), true)
		}

		// so far we assume that only strings can be ORed here
		statement := model.NewInfixExpr(model.NewColumnRef(fieldName), "==", model.NewLiteral(sprint(vUnNested)))
		return model.NewSimpleQuery(statement, true)
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return model.NewSimpleQuery(nil, false)
}

func (cw *ClickhouseQueryTranslator) parseMultiMatch(queryMap QueryMap) model.SimpleQuery {
	var fields []string
	fieldsAsInterface, ok := queryMap["fields"]
	if ok {
		if fieldsAsArray, ok := fieldsAsInterface.([]interface{}); ok {
			fields = cw.extractFields(fieldsAsArray)
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("invalid fields type: %T, value: %v", fieldsAsInterface, fieldsAsInterface)
			return model.NewSimpleQuery(nil, false)
		}
	} else {
		fields = []string{model.FullTextFieldNamePlaceHolder}
	}
	alwaysFalseStmt := model.NewLiteral("false")
	if len(fields) == 0 {
		return model.NewSimpleQuery(alwaysFalseStmt, true)
	}

	query, ok := queryMap["query"]
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("no query in multi_match query: %v", queryMap)
		return model.NewSimpleQuery(alwaysFalseStmt, false)
	}
	queryAsString, ok := query.(string)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("invalid query type: %T, value: %v", query, query)
		return model.NewSimpleQuery(alwaysFalseStmt, false)
	}
	var subQueries []string
	wereDone := false
	// 2 cases:
	// a) "type" == "phrase" -> we need to match full string
	if matchType, ok := queryMap["type"]; ok {
		if matchTypeAsString, ok := matchType.(string); ok && matchTypeAsString == "phrase" {
			wereDone = true
			subQueries = []string{queryAsString}
		}
	}
	// b) "type" == "best_fields" (or other - we treat it as default) -> we need to match any of the words
	if !wereDone {
		subQueries = strings.Split(queryAsString, " ")
	}

	sqls := make([]model.Expr, len(fields)*len(subQueries))
	i := 0
	for _, field := range fields {
		for _, subQ := range subQueries {
			simpleStat := model.NewInfixExpr(model.NewColumnRef(field), "iLIKE", model.NewLiteral("'%"+subQ+"%'"))
			sqls[i] = simpleStat
			i++
		}
	}
	return model.NewSimpleQuery(model.Or(sqls), true)
}

// prefix works only on strings
func (cw *ClickhouseQueryTranslator) parsePrefix(queryMap QueryMap) model.SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 prefix, got: %d. value: %v", len(queryMap), queryMap)
		return model.NewSimpleQuery(nil, false)
	}

	for fieldName, v := range queryMap {
		fieldName = cw.ResolveField(cw.Ctx, fieldName)
		switch vCasted := v.(type) {
		case string:
			simpleStat := model.NewInfixExpr(model.NewColumnRef(fieldName), "iLIKE", model.NewLiteral("'"+vCasted+"%'"))
			return model.NewSimpleQuery(simpleStat, true)
		case QueryMap:
			token := vCasted["value"].(string)
			simpleStat := model.NewInfixExpr(model.NewColumnRef(fieldName), "iLIKE", model.NewLiteral("'"+token+"%'"))
			return model.NewSimpleQuery(simpleStat, true)
		default:
			logger.WarnWithCtx(cw.Ctx).Msgf("unsupported prefix type: %T, value: %v", v, v)
			return model.NewSimpleQuery(nil, false)
		}
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return model.NewSimpleQuery(nil, false)
}

// Not supporting 'case_insensitive' (optional)
// Also not supporting wildcard (Required, string) (??) In both our example, and their in docs,
// it's not provided.
func (cw *ClickhouseQueryTranslator) parseWildcard(queryMap QueryMap) model.SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 wildcard, got: %d. value: %v", len(queryMap), queryMap)
		return model.NewSimpleQuery(nil, false)
	}

	for fieldName, v := range queryMap {
		fieldName = cw.ResolveField(cw.Ctx, fieldName)
		if vAsMap, ok := v.(QueryMap); ok {
			if value, ok := vAsMap["value"]; ok {
				if valueAsString, ok := value.(string); ok {
					whereStatement := model.NewInfixExpr(model.NewColumnRef(fieldName), "iLIKE", model.NewLiteral("'"+strings.ReplaceAll(valueAsString, "*", "%")+"'"))
					return model.NewSimpleQuery(whereStatement, true)
				} else {
					logger.WarnWithCtx(cw.Ctx).Msgf("invalid value type: %T, value: %v", value, value)
					return model.NewSimpleQuery(nil, false)
				}
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("no value in wildcard query: %v", queryMap)
				return model.NewSimpleQuery(nil, false)
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid wildcard type: %T, value: %v", v, v)
			return model.NewSimpleQuery(nil, false)
		}
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return model.NewSimpleQuery(nil, false)
}

// This one is really complicated (https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html)
// `query` uses Lucene language, we don't support 100% of it, but most.
func (cw *ClickhouseQueryTranslator) parseQueryString(queryMap QueryMap) model.SimpleQuery {
	var fields []string
	if fieldsRaw, ok := queryMap["fields"]; ok {
		fields = cw.extractFields(fieldsRaw.([]interface{}))
	} else {
		fields = []string{model.FullTextFieldNamePlaceHolder}
	}

	query := queryMap["query"].(string) // query: (Required, string)

	// we always call `TranslateToSQL` - Lucene parser returns "false" in case of invalid query
	whereStmtFromLucene := lucene.TranslateToSQL(cw.Ctx, query, fields, cw.Schema)
	return model.NewSimpleQuery(whereStmtFromLucene, true)
}

func (cw *ClickhouseQueryTranslator) parseNested(queryMap QueryMap) model.SimpleQuery {
	if query, ok := queryMap["query"]; ok {
		if queryAsMap, ok := query.(QueryMap); ok {
			return cw.parseQueryMap(queryAsMap)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid nested query type: %T, value: %v", query, query)
			return model.NewSimpleQuery(nil, false)
		}
	}

	logger.WarnWithCtx(cw.Ctx).Msgf("no query in nested query: %v", queryMap)
	return model.NewSimpleQuery(nil, false)
}

func (cw *ClickhouseQueryTranslator) parseDateMathExpression(expr string) (string, error) {
	expr = strings.ReplaceAll(expr, "'", "")

	exp, err := ParseDateMathExpression(expr)
	if err != nil {
		return "", err
	}

	builder := DateMathExpressionRendererFactory(cw.DateMathRenderer)
	if builder == nil {
		return "", fmt.Errorf("no date math expression renderer found: %s", cw.DateMathRenderer)
	}

	sql, err := builder.RenderSQL(exp)
	if err != nil {
		return "", err
	}

	return sql, nil
}

// DONE: tested in CH, it works for date format 'YYYY-MM-DDTHH:MM:SS.SSSZ'
// TODO:
//   - check if parseDateTime64BestEffort really works for our case (it should)
//   - implement "needed" date functions like now, now-1d etc.
func (cw *ClickhouseQueryTranslator) parseRange(queryMap QueryMap) model.SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 range, got: %d. value: %v", len(queryMap), queryMap)
		return model.NewSimpleQuery(nil, false)
	}

	// Maybe change to false if numeric fields exist.
	// Even so, most likely >99% of ranges will be dates, as they come in (almost) every request.
	const dateInSchemaExpected = true

	for fieldName, v := range queryMap {
		fieldName = cw.ResolveField(cw.Ctx, fieldName)

		fieldType := cw.Table.GetDateTimeType(cw.Ctx, cw.ResolveField(cw.Ctx, fieldName), dateInSchemaExpected)
		stmts := make([]model.Expr, 0)
		if _, ok := v.(QueryMap); !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid range type: %T, value: %v", v, v)
			continue
		}

		keysSorted := util.MapKeysSorted(v.(QueryMap))
		for _, op := range keysSorted {
			valueRaw := v.(QueryMap)[op]
			value := sprint(valueRaw)
			defaultValue := model.NewLiteral(value)
			dateManager := NewDateManager(cw.Ctx)

			// Three stages:
			// 1. dateManager.ParseDateUsualFormat
			// 2. cw.parseDateMathExpression
			// 3. just a number
			// Dates use 1-3 and finish as soon as any succeeds
			// Numbers use just 3rd

			var finalValue model.Expr
			doneParsing, isQuoted := false, len(value) > 2 && value[0] == '\'' && value[len(value)-1] == '\''
			switch fieldType {
			case clickhouse.DateTime, clickhouse.DateTime64:
				// TODO add support for "time_zone" parameter in ParseDateUsualFormat
				finalValue, doneParsing = dateManager.ParseDateUsualFormat(value, fieldType)  // stage 1
				if !doneParsing && (op == "gte" || op == "lte" || op == "gt" || op == "lt") { // stage 2
					parsed, err := cw.parseDateMathExpression(value)
					if err == nil {
						doneParsing = true
						finalValue = model.NewLiteral(parsed)
					}
				}
				if !doneParsing && isQuoted { // stage 3
					finalValue, doneParsing = dateManager.ParseDateUsualFormat(value[1:len(value)-1], fieldType)
				}
			case clickhouse.Invalid:
				if isQuoted {
					isNumber, unquoted := true, value[1:len(value)-1]
					for _, c := range unquoted {
						if !unicode.IsDigit(c) && c != '.' {
							isNumber = false
						}
					}
					if isNumber {
						finalValue = model.NewLiteral(unquoted)
						doneParsing = true
					}
				}
			default:
				logger.ErrorWithCtx(cw.Ctx).Msgf("invalid DateTime type for field: %s, parsed dateTime value: %s", fieldName, value)
			}

			if !doneParsing {
				finalValue = defaultValue
			}

			field := model.NewColumnRef(fieldName)
			switch op {
			case "gte":
				stmt := model.NewInfixExpr(field, ">=", finalValue)
				stmts = append(stmts, stmt)
			case "lte":
				stmt := model.NewInfixExpr(field, "<=", finalValue)
				stmts = append(stmts, stmt)
			case "gt":
				stmt := model.NewInfixExpr(field, ">", finalValue)
				stmts = append(stmts, stmt)
			case "lt":
				stmt := model.NewInfixExpr(field, "<", finalValue)
				stmts = append(stmts, stmt)
			case "format":
				// ignored
			default:
				logger.WarnWithCtx(cw.Ctx).Msgf("invalid range operator: %s", op)
			}
		}
		return model.NewSimpleQuery(model.And(stmts), true)
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return model.NewSimpleQuery(nil, false)
}

// TODO: not supported:
// - The field has "index" : false and "doc_values" : false set in the mapping
// - The length of the field value exceeded an ignore_above setting in the mapping
// - The field value was malformed and ignore_malformed was defined in the mapping
func (cw *ClickhouseQueryTranslator) parseExists(queryMap QueryMap) model.SimpleQuery {
	//sql := model.NewSimpleStatement("")
	var sql model.Expr
	for _, v := range queryMap {
		fieldName, ok := v.(string)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid exists type: %T, value: %v", v, v)
			return model.NewSimpleQuery(nil, false)
		}
		fieldName = cw.ResolveField(cw.Ctx, fieldName)
		fieldNameQuoted := strconv.Quote(fieldName)

		switch cw.Table.GetFieldInfo(cw.Ctx, cw.ResolveField(cw.Ctx, fieldName)) {
		case clickhouse.ExistsAndIsBaseType:
			sql = model.NewInfixExpr(model.NewColumnRef(fieldName), "IS", model.NewLiteral("NOT NULL"))
		case clickhouse.ExistsAndIsArray:
			sql = model.NewInfixExpr(model.NewNestedProperty(
				model.NewColumnRef(fieldName),
				model.NewLiteral("size0"),
			), "=", model.NewLiteral("0"))
		case clickhouse.NotExists:
			// TODO this is a workaround for the case when the field is a point
			schemaInstance := cw.Schema
			if value, ok := schemaInstance.ResolveFieldByInternalName(fieldName); ok && value.Type.Equal(schema.QuesmaTypePoint) {
				return model.NewSimpleQuery(sql, true)
			}

			attrs := cw.Table.GetAttributesList()
			stmts := make([]model.Expr, len(attrs))
			for i, a := range attrs {
				hasFunc := model.NewFunction("has", []model.Expr{model.NewColumnRef(a.KeysArrayName), model.NewColumnRef(fieldName)}...)
				arrayAccess := model.NewArrayAccess(model.NewColumnRef(a.ValuesArrayName), model.NewFunction("indexOf", []model.Expr{model.NewColumnRef(a.KeysArrayName), model.NewLiteral(fieldNameQuoted)}...))
				isNotNull := model.NewInfixExpr(arrayAccess, "IS", model.NewLiteral("NOT NULL"))
				compoundStatementNoFieldName := model.NewInfixExpr(hasFunc, "AND", isNotNull)
				stmts[i] = compoundStatementNoFieldName
			}
			sql = model.Or(stmts)
		default:
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid field type: %T for exists: %s", cw.Table.GetFieldInfo(cw.Ctx, cw.ResolveField(cw.Ctx, fieldName)), fieldName)
		}
	}
	return model.NewSimpleQuery(sql, true)
}

// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html
// We don't look at any parameter other than "value" (which is required, and is a regex pattern)
// We log warning if any other parameter arrives
func (cw *ClickhouseQueryTranslator) parseRegexp(queryMap QueryMap) (result model.SimpleQuery) {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 regexp, got: %d. value: %v", len(queryMap), queryMap)
		return
	}

	// really simple == (out of all special characters, only . and .* may be present)
	isPatternReallySimple := func(pattern string) bool {
		// any special characters excluding . and * not allowed. Also (not the most important check) * can't be first character.
		if strings.ContainsAny(pattern, `?+|{}[]()"\`) || (len(pattern) > 0 && pattern[0] == '*') {
			return false
		}
		// .* allowed, but [any other char]* - not
		for i, char := range pattern[1:] {
			if char == '*' && pattern[i] != '.' {
				return false
			}
		}
		return true
	}

	for fieldName, parametersRaw := range queryMap {
		parameters, ok := parametersRaw.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid regexp parameters type: %T, value: %v", parametersRaw, parametersRaw)
			return
		}
		patternRaw, exists := parameters["value"]
		if !exists {
			logger.WarnWithCtx(cw.Ctx).Msgf("no value in regexp query: %v", queryMap)
			return
		}
		pattern, ok := patternRaw.(string)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid pattern type: %T, value: %v", patternRaw, patternRaw)
			return
		}

		if len(parameters) > 1 {
			logger.WarnWithCtx(cw.Ctx).Msgf("unsupported regexp parameters: %v", parameters)
		}

		var funcName string
		if isPatternReallySimple(pattern) {
			pattern = strings.ReplaceAll(pattern, "_", `\_`)
			pattern = strings.ReplaceAll(pattern, ".*", "%")
			pattern = strings.ReplaceAll(pattern, ".", "_")
			funcName = "LIKE"
		} else { // this Clickhouse function is much slower, so we use it only for complex regexps
			funcName = "REGEXP"
		}
		return model.NewSimpleQuery(
			model.NewInfixExpr(model.NewColumnRef(fieldName), funcName, model.NewLiteral("'"+pattern+"'")), true)
	}

	logger.ErrorWithCtx(cw.Ctx).Msg("parseRegexp: theoretically unreachable code")
	return
}

func (cw *ClickhouseQueryTranslator) extractFields(fields []interface{}) []string {
	result := make([]string, 0)
	for _, field := range fields {
		fieldStr, ok := field.(string)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid field type: %T, value: %v", field, field)
			continue
		}
		if fieldStr == "*" {
			return []string{model.FullTextFieldNamePlaceHolder}
		}
		fieldStr = cw.ResolveField(cw.Ctx, fieldStr)
		result = append(result, fieldStr)
	}
	return result
}

// sprint is a helper function to convert interface{} to string in a way that Clickhouse can understand it
func sprint(i interface{}) string {
	switch i.(type) {
	case string:
		return fmt.Sprintf("'%v'", i)
	case QueryMap:
		iface := i
		mapType := iface.(QueryMap)
		value := mapType["value"]
		return sprint(value)
	default:
		return fmt.Sprintf("%v", i)
	}
}

// Return value:
// - listByField: (ListByField, field name, 0, LIMIT)
// - listAllFields: (ListAllFields, "*", 0, LIMIT) (LIMIT = how many rows we want to return)
func (cw *ClickhouseQueryTranslator) tryProcessSearchMetadata(queryMap QueryMap) model.HitsCountInfo {
	metadata := cw.parseMetadata(queryMap) // TODO we can remove this if we need more speed. It's a bit unnecessary call, at least for now, when we're parsing brutally.

	// maybe it's ListByField ListAllFields request
	if queryInfo, ok := cw.isItListRequest(metadata); ok {
		return queryInfo
	}

	// otherwise: None
	return model.NewEmptyHitsCountInfo()
}

// 'queryMap' - metadata part of the JSON query
// returns (info, true) if metadata shows it's ListAllFields or ListByField request (used e.g. for listing all rows in Kibana)
// returns (model.NewEmptyHitsCountInfo, false) if it's not ListAllFields/ListByField request
func (cw *ClickhouseQueryTranslator) isItListRequest(queryMap QueryMap) (model.HitsCountInfo, bool) {
	// 1) case: very simple SELECT * kind of request
	size := cw.parseSize(queryMap, model.DefaultSizeListQuery)
	if size == 0 {
		return model.NewEmptyHitsCountInfo(), false
	}

	fields, ok := queryMap["fields"].([]any)
	if !ok {
		return model.HitsCountInfo{Typ: model.ListAllFields, RequestedFields: []string{"*"}, Size: size}, true
	}
	if len(fields) > 1 {
		fieldNames := make([]string, 0)
		for _, field := range fields {
			if fieldMap, ok := field.(QueryMap); ok {
				fieldNameAsAny, ok := fieldMap["field"]
				if !ok {
					logger.WarnWithCtx(cw.Ctx).Msgf("no field in field map: %v. Skipping", fieldMap)
					continue
				}
				if fieldName, ok := fieldNameAsAny.(string); ok {
					fieldNames = append(fieldNames, fieldName)
				} else {
					logger.WarnWithCtx(cw.Ctx).Msgf("invalid field type: %T, value: %v. Expected string. Skipping", fieldName, fieldName)
				}
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("invalid field type: %T, value: %v. Expected QueryMap", field, field)
				return model.NewEmptyHitsCountInfo(), false
			}
		}
		logger.Debug().Msgf("requested more than one field %s, falling back to '*'", fieldNames)
		// so far everywhere I've seen, > 1 field ==> "*" is one of them
		return model.HitsCountInfo{Typ: model.ListAllFields, RequestedFields: []string{"*"}, Size: size}, true
	} else if len(fields) == 0 {
		// isCount, ok := queryMap["track_total_hits"].(bool)
		// TODO make count separate!
		/*
			if ok && isCount {
				return model.HitsCountInfo{Typ: model.CountAsync, RequestedFields: make([]string, 0), FieldName: "", I1: 0, I2: 0}, true
			}
		*/
		return model.NewEmptyHitsCountInfo(), false
	} else {
		// 2 cases are possible:
		// a) just a string
		fieldName, ok := fields[0].(string)
		if !ok {
			queryMap, ok = fields[0].(QueryMap)
			if !ok {
				return model.NewEmptyHitsCountInfo(), false
			}
			// b) {"field": fieldName}
			if field, ok := queryMap["field"]; ok {
				if fieldName, ok = field.(string); !ok {
					logger.WarnWithCtx(cw.Ctx).Msgf("invalid field type: %T, value: %v. Expected string", field, field)
					return model.NewEmptyHitsCountInfo(), false
				}
			} else {
				return model.NewEmptyHitsCountInfo(), false
			}
		}

		resolvedField := cw.ResolveField(cw.Ctx, fieldName)
		if resolvedField == "*" {
			return model.HitsCountInfo{Typ: model.ListAllFields, RequestedFields: []string{"*"}, Size: size}, true
		}
		return model.HitsCountInfo{Typ: model.ListByField, RequestedFields: []string{resolvedField}, Size: size}, true
	}
}

func (cw *ClickhouseQueryTranslator) extractInterval(queryMap QueryMap) (interval string, intervalType bucket_aggregations.DateHistogramIntervalType) {
	const defaultInterval = "30s"
	const defaultIntervalType = bucket_aggregations.DateHistogramFixedInterval
	if fixedInterval, exists := queryMap["fixed_interval"]; exists {
		if asString, ok := fixedInterval.(string); ok {
			return asString, bucket_aggregations.DateHistogramFixedInterval
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("unexpected type of interval: %T, value: %v. Returning default", fixedInterval, fixedInterval)
			return defaultInterval, bucket_aggregations.DateHistogramFixedInterval
		}
	}
	if calendarInterval, exists := queryMap["calendar_interval"]; exists {
		if asString, ok := calendarInterval.(string); ok {
			return asString, bucket_aggregations.DateHistogramCalendarInterval
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("unexpected type of interval: %T, value: %v. Returning default", calendarInterval, calendarInterval)
			return defaultInterval, bucket_aggregations.DateHistogramCalendarInterval
		}
	}

	// this should NEVER happen (query should always have either fixed_interval, or calendar_interval_field), so defaultIntervalType is totally arbitrary
	logger.WarnWithCtx(cw.Ctx).Msgf("extractInterval: no interval found, returning default: %s (%s)", defaultInterval, defaultIntervalType.String(cw.Ctx))
	return defaultInterval, defaultIntervalType
}

// parseSortFields parses sort fields from the query
// We're skipping ELK internal fields, like "_doc", "_id", etc. (we only accept field starting with "_" if it exists in our table)
func (cw *ClickhouseQueryTranslator) parseSortFields(sortMaps any) (sortColumns []model.OrderByExpr) {
	sortColumns = make([]model.OrderByExpr, 0)
	switch sortMaps := sortMaps.(type) {
	case []any:
		for _, sortMapAsAny := range sortMaps {
			sortMap, ok := sortMapAsAny.(QueryMap)
			if !ok {
				logger.WarnWithCtx(cw.Ctx).Msgf("parseSortFields: unexpected type of value: %T, value: %v", sortMapAsAny, sortMapAsAny)
				continue
			}

			// sortMap has only 1 key, so we can just iterate over it
			for k, v := range sortMap {
				// TODO replace cw.Table.GetFieldInfo with schema.Field[]
				if strings.HasPrefix(k, "_") && cw.Table.GetFieldInfo(cw.Ctx, cw.ResolveField(cw.Ctx, k)) == clickhouse.NotExists {
					// we're skipping ELK internal fields, like "_doc", "_id", etc.
					continue
				}
				fieldName := cw.ResolveField(cw.Ctx, k)
				switch v := v.(type) {
				case QueryMap:
					if order, ok := v["order"]; ok {
						if orderAsString, ok := order.(string); ok {
							if col, err := createSortColumn(fieldName, orderAsString); err == nil {
								sortColumns = append(sortColumns, col)
							} else {
								logger.WarnWithCtx(cw.Ctx).Msg(err.Error())
							}
						} else {
							logger.WarnWithCtx(cw.Ctx).Msgf("unexpected order type: %T, value: %v. Skipping", order, order)
						}
					} else {
						sortColumns = append(sortColumns, model.NewSortColumn(fieldName, model.AscOrder))
					}
				case string:
					if col, err := createSortColumn(fieldName, v); err == nil {
						sortColumns = append(sortColumns, col)
					} else {
						logger.WarnWithCtx(cw.Ctx).Msg(err.Error())
					}
				default:
					logger.WarnWithCtx(cw.Ctx).Msgf("unexpected 'sort' value's type: %T (key, value): (%s, %v). Skipping", v, k, v)
				}
			}
		}
		return sortColumns
	case map[string]interface{}:
		for fieldName, fieldValue := range sortMaps {
			if strings.HasPrefix(fieldName, "_") && cw.Table.GetFieldInfo(cw.Ctx, cw.ResolveField(cw.Ctx, fieldName)) == clickhouse.NotExists {
				// TODO Elastic internal fields will need to be supported in the future
				continue
			}
			if fieldValue, ok := fieldValue.(string); ok {
				if col, err := createSortColumn(fieldName, fieldValue); err == nil {
					sortColumns = append(sortColumns, col)
				} else {
					logger.WarnWithCtx(cw.Ctx).Msg(err.Error())
				}
			}
		}

		return sortColumns

	case map[string]string:
		for fieldName, fieldValue := range sortMaps {
			if strings.HasPrefix(fieldName, "_") && cw.Table.GetFieldInfo(cw.Ctx, cw.ResolveField(cw.Ctx, fieldName)) == clickhouse.NotExists {
				// TODO Elastic internal fields will need to be supported in the future
				continue
			}
			if col, err := createSortColumn(fieldName, fieldValue); err == nil {
				sortColumns = append(sortColumns, col)
			} else {
				logger.WarnWithCtx(cw.Ctx).Msg(err.Error())
			}
		}

		return sortColumns
	default:
		logger.ErrorWithCtx(cw.Ctx).Msgf("unexpected type of sortMaps: %T, value: %v", sortMaps, sortMaps)
		return []model.OrderByExpr{}
	}
}

func createSortColumn(fieldName, ordering string) (model.OrderByExpr, error) {
	ordering = strings.ToLower(ordering)
	switch ordering {
	case "asc":
		return model.NewSortColumn(fieldName, model.AscOrder), nil
	case "desc":
		return model.NewSortColumn(fieldName, model.DescOrder), nil
	default:
		return model.OrderByExpr{}, fmt.Errorf("unexpected order value: [%s] for field [%s] Skipping", ordering, fieldName)
	}
}

// ResolveField resolves field name to internal name
// For now, it's part of QueryParser, however, it can
// be part of transformation pipeline in the future
// What prevents us from moving it to transformation pipeline now, is that
// we need to anotate this field somehow in the AST, to be able
// to distinguish it from other fields
func (cw *ClickhouseQueryTranslator) ResolveField(ctx context.Context, fieldName string) string {
	// Alias resolution should occur *after* the query is parsed, not during the parsing

	schemaInstance := cw.Schema

	fieldName = strings.TrimSuffix(fieldName, ".keyword")
	fieldName = strings.TrimSuffix(fieldName, ".text")

	if resolvedField, ok := schemaInstance.ResolveField(fieldName); ok {
		return resolvedField.InternalPropertyName.AsString()
	} else {
		if fieldName != "*" && fieldName != "_all" && fieldName != "_doc" && fieldName != "_id" && fieldName != "_index" {
			logger.DebugWithCtx(ctx).Msgf("field '%s' referenced, but not found in schema, falling back to original name", fieldName)
		}

		return fieldName
	}
}

func (cw *ClickhouseQueryTranslator) parseSize(queryMap QueryMap, defaultSize int) int {
	sizeRaw, exists := queryMap["size"]
	if !exists {
		return defaultSize
	} else if sizeAsFloat, ok := sizeRaw.(float64); ok {
		return int(sizeAsFloat)
	} else if sizeAsString, ok := sizeRaw.(string); ok {
		if sizeAsInt, err := strconv.Atoi(sizeAsString); err == nil {
			return sizeAsInt
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid size type: %T, value: %v. Expected int", sizeRaw, sizeRaw)
			return defaultSize
		}
	} else {
		logger.WarnWithCtx(cw.Ctx).Msgf("invalid size type: %T, value: %v. Expected float64", sizeRaw, sizeRaw)
		return defaultSize
	}
}

func (cw *ClickhouseQueryTranslator) GetDateTimeTypeFromSelectClause(ctx context.Context, expr model.Expr,
	dateInSchemaExpected bool) clickhouse.DateTimeType {
	if ref, ok := expr.(model.ColumnRef); ok {
		return cw.Table.GetDateTimeType(ctx, cw.ResolveField(ctx, ref.ColumnName), dateInSchemaExpected)
	}
	return clickhouse.Invalid
}

func (cw *ClickhouseQueryTranslator) parseGeoBoundingBox(queryMap QueryMap) model.SimpleQuery {
	stmts := make([]model.Expr, 0)
	bottomRightExpressions := make([]model.Expr, 0)
	topLeftExpressions := make([]model.Expr, 0)
	var field string
	for k, v := range queryMap {
		// TODO handle lat lon as array case for now
		// Generate following where statement, assuming that field
		// is equal to "Location"
		// GEO_BOUNDING_BOX("Location", top_left_lat, top_left_lon, bottom_right_lat, bottom_right_lon))
		// GEO_BOUNDING_BOX here is an abstract geo function that will be mapped
		// later to specific Clickhouse (or any other db function in the future)
		// it takes 5 arguments: field, topLeftLat, topLeftLon, bottomRightLat, bottomRightLon
		field = k
		if bottomRight, ok := v.(QueryMap)["bottom_right"]; ok {
			if bottomRightCornerAsArray, ok := bottomRight.([]interface{}); ok {
				bottomRightExpressions = append(bottomRightExpressions, model.NewLiteral(fmt.Sprintf("%v", bottomRightCornerAsArray[0])))
				bottomRightExpressions = append(bottomRightExpressions, model.NewLiteral(fmt.Sprintf("%v", bottomRightCornerAsArray[1])))
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("no bottom_right in geo_bounding_box query: %v", queryMap)
			return model.NewSimpleQuery(nil, false)
		}
		if topLeft, ok := v.(QueryMap)["top_left"]; ok {
			if topLeftCornerAsArray, ok := topLeft.([]interface{}); ok {
				topLeftExpressions = append(topLeftExpressions, model.NewLiteral(fmt.Sprintf("%v", topLeftCornerAsArray[0])))
				topLeftExpressions = append(topLeftExpressions, model.NewLiteral(fmt.Sprintf("%v", topLeftCornerAsArray[1])))
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("no top_left in geo_bounding_box query: %v", queryMap)
			return model.NewSimpleQuery(nil, false)
		}
		args := make([]model.Expr, 0)
		args = append(args, model.NewColumnRef(field))
		args = append(args, topLeftExpressions...)
		args = append(args, bottomRightExpressions...)
		fun := model.NewFunction("GEO_BOUNDING_BOX", args...)
		_ = fun
		// TODO uncomment when GEO_BOUNDING_BOX is implemented
		// it requires additional transformation to update field names
		//stmts = append(stmts, fun)
	}
	return model.NewSimpleQuery(model.And(stmts), true)
}
