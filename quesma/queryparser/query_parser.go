package queryparser

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/lucene"
	"mitmproxy/quesma/queryparser/query_util"
	wc "mitmproxy/quesma/queryparser/where_clause"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/util"
	"strconv"
	"strings"
	"unicode"

	"github.com/k0kubun/pp"
	"github.com/relvacode/iso8601"
)

var stringRenderer = &wc.StringRenderer{}

type QueryMap = map[string]interface{}

// NewEmptyHighlighter returns no-op for error branches and tests
func NewEmptyHighlighter() model.Highlighter {
	return model.Highlighter{
		Fields: make(map[string]bool),
	}
}

func (cw *ClickhouseQueryTranslator) ParseQuery(body types.JSON) ([]model.Query, bool, bool, error) {
	simpleQuery, queryInfo, highlighter, err := cw.ParseQueryInternal(body)

	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Msgf("error parsing query: %v", err)
		return nil, false, false, err
	}

	var query *model.Query
	var queries []model.Query
	var isAggregation bool
	canParse := false

	if simpleQuery.CanParse {
		canParse = true
		if query_util.IsNonAggregationQuery(queryInfo, body) {
			query = cw.makeBasicQuery(simpleQuery, queryInfo, highlighter)
			query.SortFields = simpleQuery.SortFields
			queries = append(queries, *query)
			isAggregation = false
			return queries, isAggregation, canParse, nil
		} else {
			queries, err = cw.ParseAggregationJson(body)
			if err != nil {
				logger.ErrorWithCtx(cw.Ctx).Msgf("error parsing aggregation: %v", err)
				return nil, false, false, err
			}
			isAggregation = true
			return queries, isAggregation, canParse, nil
		}
	}

	return nil, false, false, err
}

func (cw *ClickhouseQueryTranslator) makeBasicQuery(
	simpleQuery model.SimpleQuery, queryInfo model.SearchQueryInfo, highlighter model.Highlighter) *model.Query {
	var fullQuery *model.Query

	var whereClause string
	if simpleQuery.Sql.WhereStatement == nil {
		whereClause = ""
	} else {
		whereClause = simpleQuery.Sql.WhereStatement.Accept(stringRenderer).(string)
	}
	switch queryInfo.Typ {
	case model.CountAsync:
		fullQuery = cw.BuildSimpleCountQuery(whereClause)
	case model.Facets, model.FacetsNumeric:
		// queryInfo = (Facets, fieldName, Limit results, Limit last rows to look into)
		fullQuery = cw.BuildFacetsQuery(queryInfo.FieldName, whereClause)
	case model.ListByField:
		// queryInfo = (ListByField, fieldName, 0, LIMIT)
		fullQuery = cw.BuildNRowsQuery(queryInfo.FieldName, simpleQuery, queryInfo.I2)
	case model.ListAllFields:
		// queryInfo = (ListAllFields, "*", 0, LIMIT)
		fullQuery = cw.BuildNRowsQuery("*", simpleQuery, queryInfo.I2)
	case model.Normal:
		fullQuery = cw.BuildNRowsQuery("*", simpleQuery, queryInfo.I2)
	}
	fullQuery.QueryInfo = queryInfo
	fullQuery.Highlighter = highlighter
	return fullQuery
}

func (cw *ClickhouseQueryTranslator) ParseQueryInternal(body types.JSON) (model.SimpleQuery, model.SearchQueryInfo, model.Highlighter, error) {
	queryAsMap := body.Clone()
	cw.ClearTokensToHighlight()

	// we must parse "highlights" here, because it is stripped from the queryAsMap later
	highlighter := cw.ParseHighlighter(queryAsMap)

	var parsedQuery model.SimpleQuery
	if queryPart, ok := queryAsMap["query"]; ok {
		parsedQuery = cw.parseQueryMap(queryPart.(QueryMap))
	} else {
		parsedQuery = model.NewSimpleQuery(model.NewSimpleStatement(""), true)
	}

	if sortPart, ok := queryAsMap["sort"]; ok {
		parsedQuery.SortFields = cw.parseSortFields(sortPart)
	}

	const defaultSize = 0
	size := defaultSize
	if sizeRaw, ok := queryAsMap["size"]; ok {
		if sizeFloat, ok := sizeRaw.(float64); ok {
			size = int(sizeFloat)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("unknown size format, size value: %v type: %T. Using default (%d)", sizeRaw, sizeRaw, defaultSize)
		}
	}

	queryInfo := cw.tryProcessSearchMetadata(queryAsMap)
	queryInfo.Size = size

	highlighter.SetTokens(cw.tokensToHighlight)
	cw.ClearTokensToHighlight()

	return parsedQuery, queryInfo, highlighter, nil
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

	highlighter.Fields = make(map[string]bool)
	for k, v := range cw.Table.Cols {
		if v.IsFullTextMatch {
			highlighter.Fields[k] = true
		}
	}

	return highlighter
}

func (cw *ClickhouseQueryTranslator) ParseQueryAsyncSearch(queryAsJson string) (model.SimpleQuery, model.SearchQueryInfo, model.Highlighter) {
	cw.ClearTokensToHighlight()
	queryAsMap, err := types.ParseJSON(queryAsJson)
	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("error parsing query request's JSON")
		return model.NewSimpleQuery(model.NewSimpleStatement("invalid JSON (ParseQueryAsyncSearch)"), false), model.NewSearchQueryInfoNone(), NewEmptyHighlighter()
	}

	// we must parse "highlights" here, because it is stripped from the queryAsMap later
	highlighter := cw.ParseHighlighter(queryAsMap)

	var parsedQuery model.SimpleQuery
	if query, ok := queryAsMap["query"]; ok {
		queryMap, ok := query.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid query type: %T, value: %v", query, query)
			return model.NewSimpleQuery(model.NewSimpleStatement("invalid query type"), false), model.NewSearchQueryInfoNone(), NewEmptyHighlighter()
		}
		parsedQuery = cw.parseQueryMap(queryMap)
	} else {
		return model.NewSimpleQuery(model.NewSimpleStatement(""), true), cw.tryProcessSearchMetadata(queryAsMap), highlighter
	}

	if sort, ok := queryAsMap["sort"]; ok {
		parsedQuery.SortFields = cw.parseSortFields(sort)
	}
	queryInfo := cw.tryProcessSearchMetadata(queryAsMap)

	highlighter.SetTokens(cw.tokensToHighlight)
	cw.ClearTokensToHighlight()

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
	fieldName = cw.Table.ResolveField(cw.Ctx, fieldName)
	canParse := true
	stmts := make([]model.Statement, 0)
	if indexFilter != nil {
		res := cw.parseQueryMap(*indexFilter)
		canParse = res.CanParse
		stmts = append(stmts, res.Sql)
	}
	if prefix != nil && len(*prefix) > 0 {
		// Maybe quote it?
		var like string
		if caseIns {
			like = "iLIKE"
		} else {
			like = "LIKE"
		}
		cw.AddTokenToHighlight(*prefix)
		simpleStat := model.NewSimpleStatement(fieldName + " " + like + " '" + *prefix + "%'")
		simpleStat.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(fieldName), like, wc.NewLiteral("'"+*prefix+"%'"))
		stmts = append(stmts, simpleStat)
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
		return model.NewSimpleQuery(model.NewSimpleStatement(""), true)
	}

	// if we can't parse the query, we should show the bug
	unparsedQuery := pp.Sprint(queryMap)
	if prettyMarshal, err := json.Marshal(queryMap); err == nil {
		unparsedQuery = string(prettyMarshal)
	}
	return model.NewSimpleQuery(model.NewSimpleStatement("can't parse query: "+unparsedQuery), false)
}

// `constant_score` query is just a wrapper for filter query which returns constant relevance score, which we ignore anyway
func (cw *ClickhouseQueryTranslator) parseConstantScore(queryMap QueryMap) model.SimpleQuery {
	if _, ok := queryMap["filter"]; ok {
		return cw.parseBool(queryMap)
	} else {
		return model.NewSimpleQuery(model.NewSimpleStatement("parsing error: `constant_score` needs to wrap `filter` query"), false)
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
		return model.NewSimpleQuery(model.NewSimpleStatement("parsing error: missing mandatory `values` field"), false)
	}
	logger.Warn().Msgf("unsupported id query executed, requested ids of [%s]", strings.Join(ids, "','"))

	timestampColumnName, err := cw.GetTimestampFieldName()
	if err != nil {
		logger.Warn().Msgf("id query executed, but not timestamp field configured")
		return model.NewSimpleQuery(model.NewSimpleStatement(""), true)
	}
	if len(ids) == 0 {
		return model.NewSimpleQuery(model.NewSimpleStatement("parsing error: empty _id array"), false)
	}

	// when our generated ID appears in query looks like this: `1d<TRUNCATED>0b8q1`
	// therefore we need to strip the hex part (before `q`) and convert it to decimal
	// then we can query at DB level
	for i, id := range ids {
		idInHex := strings.Split(id, "q")[0]
		if idAsStr, err := hex.DecodeString(idInHex); err != nil {
			logger.Error().Msgf("error parsing document id %s: %v", id, err)
			return model.NewSimpleQuery(model.NewSimpleStatement(""), true)
		} else {
			tsWithoutTZ := strings.TrimSuffix(string(idAsStr), " +0000 UTC")
			ids[i] = fmt.Sprintf("'%s'", tsWithoutTZ)
		}
	}

	var statement model.Statement
	if v, ok := cw.Table.Cols[timestampColumnName]; ok {
		switch v.Type.String() {
		case clickhouse.DateTime64.String():
			for _, id := range ids {
				finalIds = append(finalIds, fmt.Sprintf("toDateTime64(%s,3)", id))
			}
			if len(finalIds) == 1 {
				statement = model.NewSimpleStatement(fmt.Sprintf("%s = %s", strconv.Quote(timestampColumnName), finalIds[0]))
				statement.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(timestampColumnName), " = ", wc.NewFunction("toDateTime64", wc.NewLiteral(ids[0]), wc.NewLiteral("3")))
			} else {
				statement = model.NewSimpleStatement(fmt.Sprintf("%s IN (%s)", strconv.Quote(timestampColumnName), strings.Join(finalIds, ",")))
				statement.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(timestampColumnName), " IN ", wc.NewFunction("toDateTime64", wc.NewLiteral(strings.Join(ids, ",")), wc.NewLiteral("3")))
			}
		case clickhouse.DateTime.String():
			for _, id := range ids {
				finalIds = append(finalIds, fmt.Sprintf("toDateTime(%s)", id))
			}
			if len(finalIds) == 1 {
				statement = model.NewSimpleStatement(fmt.Sprintf("%s = (%s)", strconv.Quote(timestampColumnName), finalIds[0]))
				statement.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(timestampColumnName), " = ", wc.NewFunction("toDateTime", wc.NewLiteral(finalIds[0])))
			} else {
				statement = model.NewSimpleStatement(fmt.Sprintf("%s IN (%s)", strconv.Quote(timestampColumnName), strings.Join(finalIds, ",")))
				statement.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(timestampColumnName), " IN ", wc.NewFunction("toDateTime", wc.NewLiteral(strings.Join(ids, ","))))
			}
		default:
			logger.Warn().Msgf("timestamp field of unsupported type %s", v.Type.String())
			return model.NewSimpleQuery(model.NewSimpleStatement(""), true)
		}
	}
	return model.NewSimpleQuery(statement, true)
}

// Parses each model.SimpleQuery separately, returns list of translated SQLs
func (cw *ClickhouseQueryTranslator) parseQueryMapArray(queryMaps []interface{}) (stmts []model.Statement, canParse bool) {
	stmts = make([]model.Statement, len(queryMaps))
	canParse = true
	for i, v := range queryMaps {
		if vAsMap, ok := v.(QueryMap); ok {
			query := cw.parseQueryMap(vAsMap)
			stmts[i] = query.Sql
			stmts[i].FieldName = query.FieldName
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

func (cw *ClickhouseQueryTranslator) iterateListOrDictAndParse(queryMaps interface{}) (stmts []model.Statement, canParse bool) {
	switch queryMapsTyped := queryMaps.(type) {
	case []interface{}:
		return cw.parseQueryMapArray(queryMapsTyped)
	case QueryMap:
		simpleQuery := cw.parseQueryMap(queryMapsTyped)
		return []model.Statement{simpleQuery.Sql}, simpleQuery.CanParse
	default:
		logger.WarnWithCtx(cw.Ctx).Msgf("Invalid query type: %T, value: %v", queryMapsTyped, queryMapsTyped)
		return []model.Statement{model.NewSimpleStatement("invalid iteration")}, false
	}
}

// TODO: minimum_should_match parameter. Now only ints supported and >1 changed into 1
func (cw *ClickhouseQueryTranslator) parseBool(queryMap QueryMap) model.SimpleQuery {
	var andStmts []model.Statement
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
		} else if len(orSql.Stmt) > 0 {
			sql = model.And([]model.Statement{sql, orSql})
		}
	}

	if queries, ok := queryMap["must_not"]; ok {
		sqlNots, canParseThis := cw.iterateListOrDictAndParse(queries)
		sqlNots = model.FilterNonEmpty(sqlNots)
		canParse = canParse && canParseThis
		if len(sqlNots) > 0 {
			orSql := model.Or(sqlNots)
			orSql.WhereStatement = wc.NewPrefixOp("NOT", []wc.Statement{orSql.WhereStatement})
			if orSql.IsCompound {
				orSql.Stmt = "NOT (" + orSql.Stmt + ")"
				orSql.IsCompound = false // NOT (compound) is again simple
			} else {
				orSql.Stmt = "NOT " + orSql.Stmt
			}
			sql = model.And([]model.Statement{sql, orSql})
		}
	}
	return model.NewSimpleQueryWithFieldName(sql, canParse, sql.FieldName)
}

func (cw *ClickhouseQueryTranslator) parseTerm(queryMap QueryMap) model.SimpleQuery {
	if len(queryMap) == 1 {
		for k, v := range queryMap {
			cw.AddTokenToHighlight(v)
			if k == "_index" { // index is a table name, already taken from URI and moved to FROM clause
				logger.Warn().Msgf("term %s=%v in query body, ignoring in result SQL", k, v)
				simpleStat := model.NewSimpleStatement(" 0=0 /* " + strconv.Quote(k) + "=" + sprint(v) + " */ ")
				simpleStat.WhereStatement = wc.NewInfixOp(wc.NewLiteral("0"), "=", wc.NewLiteral("0 /* "+k+"="+sprint(v)+" */"))
				return model.NewSimpleQuery(simpleStat, true)
			}
			simpleStat := model.NewSimpleStatement(strconv.Quote(k) + "=" + sprint(v))
			simpleStat.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(k), "=", wc.NewLiteral(sprint(v)))
			return model.NewSimpleQuery(simpleStat, true)
		}
	}
	logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 term, got: %d. value: %v", len(queryMap), queryMap)
	return model.NewSimpleQuery(model.NewSimpleStatement("invalid term len, != 1"), false)
}

// TODO remove optional parameters like boost
func (cw *ClickhouseQueryTranslator) parseTerms(queryMap QueryMap) model.SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 term, got: %d. value: %v", len(queryMap), queryMap)
		return model.NewSimpleQuery(model.NewSimpleStatement("invalid terms len, != 1"), false)
	}

	for k, v := range queryMap {
		if strings.HasPrefix(k, "_") {
			// terms enum API uses _tier terms ( data_hot, data_warm, etc.)
			// we don't want these internal fields to percolate to the SQL query
			return model.NewSimpleQuery(model.NewSimpleStatement(""), true)
		}
		vAsArray, ok := v.([]interface{})
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid terms type: %T, value: %v", v, v)
			return model.NewSimpleQuery(model.NewSimpleStatement("invalid terms type"), false)
		}
		if len(vAsArray) == 1 {
			simpleStatement := model.NewSimpleStatement(strconv.Quote(k) + "=" + sprint(vAsArray[0]))
			simpleStatement.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(k), "=", wc.NewLiteral(sprint(vAsArray[0])))
			return model.NewSimpleQuery(simpleStatement, true)
		}
		values := make([]string, len(vAsArray))
		for i, v := range vAsArray {
			cw.AddTokenToHighlight(v)
			values[i] = sprint(v)
		}
		combinedValues := "(" + strings.Join(values, ",") + ")"
		compoundStatement := model.NewSimpleStatement(fmt.Sprintf("%s IN %s", strconv.Quote(k), combinedValues))
		compoundStatement.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(k), "IN", wc.NewLiteral(combinedValues))
		return model.NewSimpleQuery(compoundStatement, true)
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return model.NewSimpleQuery(model.NewSimpleStatement("error, should be unreachable"), false)
}

func (cw *ClickhouseQueryTranslator) parseMatchAll(_ QueryMap) model.SimpleQuery {
	return model.NewSimpleQuery(model.NewSimpleStatement(""), true)
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
		return model.NewSimpleQuery(model.NewSimpleStatement("unsupported match len != 1"), false)
	}

	for fieldName, v := range queryMap {
		fieldName = cw.Table.ResolveField(cw.Ctx, fieldName)
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
			statements := make([]model.Statement, 0, len(subQueries))
			cw.AddTokenToHighlight(vAsString)
			for _, subQuery := range subQueries {
				cw.AddTokenToHighlight(subQuery)
				if fieldName == "_id" { // We compute this field on the fly using our custom logic, so we have to parse it differently
					computedIdMatchingQuery := cw.parseIds(QueryMap{"values": []interface{}{subQuery}})
					statements = append(statements, computedIdMatchingQuery.Sql)
				} else {
					simpleStat := model.NewSimpleStatement(strconv.Quote(fieldName) + " iLIKE " + "'%" + subQuery + "%'")
					simpleStat.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(fieldName), "iLIKE", wc.NewLiteral("'%"+subQuery+"%'"))
					statements = append(statements, simpleStat)
				}
			}
			return model.NewSimpleQuery(model.Or(statements), true)
		}

		cw.AddTokenToHighlight(vUnNested)

		// so far we assume that only strings can be ORed here
		statement := model.NewSimpleStatement(strconv.Quote(fieldName) + " == " + sprint(vUnNested))
		statement.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(fieldName), "==", wc.NewLiteral(sprint(vUnNested)))
		return model.NewSimpleQuery(statement, true)
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return model.NewSimpleQuery(model.NewSimpleStatement("error, should be unreachable"), false)
}

func (cw *ClickhouseQueryTranslator) parseMultiMatch(queryMap QueryMap) model.SimpleQuery {
	var fields []string
	fieldsAsInterface, ok := queryMap["fields"]
	if ok {
		if fieldsAsArray, ok := fieldsAsInterface.([]interface{}); ok {
			fields = cw.extractFields(fieldsAsArray)
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("invalid fields type: %T, value: %v", fieldsAsInterface, fieldsAsInterface)
			return model.NewSimpleQuery(model.NewSimpleStatement("invalid fields type"), false)
		}
	} else {
		fields = cw.Table.GetFulltextFields()
	}
	alwaysFalseStmt := model.AlwaysFalseStatement
	alwaysFalseStmt.WhereStatement = wc.NewLiteral("false")
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

	cw.AddTokenToHighlight(queryAsString)
	for _, subQ := range subQueries {
		cw.AddTokenToHighlight(subQ)
	}

	sqls := make([]model.Statement, len(fields)*len(subQueries))
	i := 0
	for _, field := range fields {
		for _, subQ := range subQueries {
			simpleStat := model.NewSimpleStatement(strconv.Quote(field) + " iLIKE '%" + subQ + "%'")
			simpleStat.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(field), "iLIKE", wc.NewLiteral("'%"+subQ+"%'"))
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
		return model.NewSimpleQuery(model.NewSimpleStatement("invalid prefix len != 1"), false)
	}

	for fieldName, v := range queryMap {
		fieldName = cw.Table.ResolveField(cw.Ctx, fieldName)
		switch vCasted := v.(type) {
		case string:
			cw.AddTokenToHighlight(vCasted)
			simpleStat := model.NewSimpleStatement(strconv.Quote(fieldName) + " iLIKE '" + vCasted + "%'")
			simpleStat.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(fieldName), "iLIKE", wc.NewLiteral("'"+vCasted+"%'"))
			return model.NewSimpleQuery(simpleStat, true)
		case QueryMap:
			token := vCasted["value"].(string)
			cw.AddTokenToHighlight(token)
			simpleStat := model.NewSimpleStatement(strconv.Quote(fieldName) + " iLIKE '" + token + "%'")
			simpleStat.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(fieldName), "iLIKE", wc.NewLiteral("'"+token+"%'"))
			return model.NewSimpleQuery(simpleStat, true)
		default:
			logger.WarnWithCtx(cw.Ctx).Msgf("unsupported prefix type: %T, value: %v", v, v)
			return model.NewSimpleQuery(model.NewSimpleStatement("unsupported prefix type"), false)
		}
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return model.NewSimpleQuery(model.NewSimpleStatement("error, should be unreachable"), false)
}

// Not supporting 'case_insensitive' (optional)
// Also not supporting wildcard (Required, string) (??) In both our example, and their in docs,
// it's not provided.
func (cw *ClickhouseQueryTranslator) parseWildcard(queryMap QueryMap) model.SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 wildcard, got: %d. value: %v", len(queryMap), queryMap)
		return model.NewSimpleQuery(model.NewSimpleStatement("invalid wildcard len != 1"), false)
	}

	for fieldName, v := range queryMap {
		fieldName = cw.Table.ResolveField(cw.Ctx, fieldName)
		if vAsMap, ok := v.(QueryMap); ok {
			if value, ok := vAsMap["value"]; ok {
				if valueAsString, ok := value.(string); ok {
					cw.AddTokenToHighlight(valueAsString)
					simpleStat := model.NewSimpleStatement(strconv.Quote(fieldName) + " iLIKE '" +
						strings.ReplaceAll(valueAsString, "*", "%") + "'")
					simpleStat.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(fieldName), "iLIKE", wc.NewLiteral("'"+strings.ReplaceAll(valueAsString, "*", "%")+"'"))
					return model.NewSimpleQuery(simpleStat, true)
				} else {
					logger.WarnWithCtx(cw.Ctx).Msgf("invalid value type: %T, value: %v", value, value)
					return model.NewSimpleQuery(model.NewSimpleStatement("invalid value type"), false)
				}
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("no value in wildcard query: %v", queryMap)
				return model.NewSimpleQuery(model.NewSimpleStatement("no value in wildcard query"), false)
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid wildcard type: %T, value: %v", v, v)
			return model.NewSimpleQuery(model.NewSimpleStatement("invalid wildcard type"), false)
		}
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return model.NewSimpleQuery(model.NewSimpleStatement("error, should be unreachable"), false)
}

// This one is really complicated (https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html)
// `query` uses Lucene language, we don't support 100% of it, but most.
func (cw *ClickhouseQueryTranslator) parseQueryString(queryMap QueryMap) model.SimpleQuery {
	var fields []string
	if fieldsRaw, ok := queryMap["fields"]; ok {
		fields = cw.extractFields(fieldsRaw.([]interface{}))
	} else {
		fields = cw.Table.GetFulltextFields()
	}

	query := queryMap["query"].(string) // query: (Required, string)

	// TODO This highlighting seems not that bad for the first version,
	// but we probably should improve it, at least a bit
	cw.AddTokenToHighlight(query)
	for _, querySubstring := range strings.Split(query, " ") {
		cw.AddTokenToHighlight(querySubstring)
	}

	// we always call `TranslateToSQL` - Lucene parser returns "false" in case of invalid query
	whereStmtFromLucene := lucene.TranslateToSQL(cw.Ctx, query, fields)
	simpleStat := model.NewSimpleStatement("")
	if whereStmtFromLucene != nil {
		simpleStat = model.NewSimpleStatement(whereStmtFromLucene.Accept(stringRenderer).(string))
	}
	simpleStat.WhereStatement = whereStmtFromLucene
	return model.NewSimpleQuery(simpleStat, true)
}

func (cw *ClickhouseQueryTranslator) parseNested(queryMap QueryMap) model.SimpleQuery {
	if query, ok := queryMap["query"]; ok {
		if queryAsMap, ok := query.(QueryMap); ok {
			return cw.parseQueryMap(queryAsMap)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid nested query type: %T, value: %v", query, query)
			return model.NewSimpleQuery(model.NewSimpleStatement("invalid nested query type"), false)
		}
	}

	logger.WarnWithCtx(cw.Ctx).Msgf("no query in nested query: %v", queryMap)
	return model.NewSimpleQuery(model.NewSimpleStatement("no query in nested query"), false)
}

func (cw *ClickhouseQueryTranslator) parseDateMathExpression(expr string) (string, error) {
	expr = strings.ReplaceAll(expr, "'", "")

	exp, err := ParseDateMathExpression(expr)
	if err != nil {
		logger.Warn().Msgf("error parsing date math expression: %s", expr)
		return "", err
	}

	builder := DateMathExpressionRendererFactory(cw.DateMathRenderer)
	if builder == nil {
		return "", fmt.Errorf("no date math expression renderer found: %s", cw.DateMathRenderer)
	}

	sql, err := builder.RenderSQL(exp)
	if err != nil {
		logger.Warn().Msgf("error rendering date math expression: %s", expr)
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
		return model.NewSimpleQuery(model.NewSimpleStatement("invalid range len != 1"), false)
	}

	for field, v := range queryMap {
		field = cw.Table.ResolveField(cw.Ctx, field)
		stmts := make([]model.Statement, 0)
		if _, ok := v.(QueryMap); !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid range type: %T, value: %v", v, v)
			continue
		}
		isDatetimeInDefaultFormat := true // in 99% requests, format is "strict_date_optional_time", which we can parse with time.Parse(time.RFC3339Nano, ..)
		if format, ok := v.(QueryMap)["format"]; ok && format == "epoch_millis" {
			isDatetimeInDefaultFormat = false
		}

		keysSorted := util.MapKeysSorted(v.(QueryMap))
		for _, op := range keysSorted {
			v := v.(QueryMap)[op]
			var fieldToPrint, timeFormatFuncName string
			var valueToCompare wc.Statement
			fieldType := cw.Table.GetDateTimeType(cw.Ctx, field)
			vToPrint := sprint(v)
			valueToCompare = wc.NewLiteral(vToPrint)
			if !isDatetimeInDefaultFormat {
				timeFormatFuncName = "toUnixTimestamp64Milli"
				fieldToPrint = "toUnixTimestamp64Milli(" + strconv.Quote(field) + ")"
			} else {
				fieldToPrint = strconv.Quote(field)
				switch fieldType {
				case clickhouse.DateTime64, clickhouse.DateTime:
					if dateTime, ok := v.(string); ok {
						// if it's a date, we need to parse it to Clickhouse's DateTime format
						// how to check if it does not contain date math expression?
						if _, err := iso8601.ParseString(dateTime); err == nil {
							vToPrint, timeFormatFuncName = cw.parseDateTimeString(cw.Table, field, dateTime)
							// TODO Investigate the quotation below
							valueToCompare = wc.NewFunction(timeFormatFuncName, wc.NewLiteral(fmt.Sprintf("'%s'", dateTime)))
						} else if op == "gte" || op == "lte" || op == "gt" || op == "lt" {
							vToPrint, err = cw.parseDateMathExpression(vToPrint)
							valueToCompare = wc.NewLiteral(vToPrint)
							if err != nil {
								logger.WarnWithCtx(cw.Ctx).Msgf("error parsing date math expression: %s", vToPrint)
								return model.NewSimpleQuery(model.NewSimpleStatement("error parsing date math expression: "+vToPrint), false)
							}
						}
					} else if v == nil {
						vToPrint = "NULL"
						valueToCompare = wc.NewLiteral("NULL")
					}
				case clickhouse.Invalid: // assumes it is number that does not need formatting
					if len(vToPrint) > 2 && vToPrint[0] == '\'' && vToPrint[len(vToPrint)-1] == '\'' {
						isNumber := true
						for _, c := range vToPrint[1 : len(vToPrint)-1] {
							if !unicode.IsDigit(c) && c != '.' {
								isNumber = false
							}
						}
						if isNumber {
							vToPrint = vToPrint[1 : len(vToPrint)-1]
						} else {
							logger.WarnWithCtx(cw.Ctx).Msgf("we use range with unknown literal %s, field %s", vToPrint, field)
						}
						valueToCompare = wc.NewLiteral(vToPrint)
					}
				default:
					logger.WarnWithCtx(cw.Ctx).Msgf("invalid DateTime type for field: %s, parsed dateTime value: %s", field, vToPrint)
				}
			}

			switch op {
			case "gte":
				simpleStat := model.NewSimpleStatement(fieldToPrint + ">=" + vToPrint)
				simpleStat.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(field), ">=", valueToCompare)
				stmts = append(stmts, simpleStat)
			case "lte":
				simpleStat := model.NewSimpleStatement(fieldToPrint + "<=" + vToPrint)
				simpleStat.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(field), "<=", valueToCompare)
				stmts = append(stmts, simpleStat)
			case "gt":
				simpleStat := model.NewSimpleStatement(fieldToPrint + ">" + vToPrint)
				simpleStat.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(field), ">", valueToCompare)
				stmts = append(stmts, simpleStat)
			case "lt":
				simpleStat := model.NewSimpleStatement(fieldToPrint + "<" + vToPrint)
				simpleStat.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(field), "<", valueToCompare)
				stmts = append(stmts, simpleStat)
			case "format":
				// ignored
			default:
				logger.WarnWithCtx(cw.Ctx).Msgf("invalid range operator: %s", op)
			}
		}
		return model.NewSimpleQueryWithFieldName(model.And(stmts), true, field)
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return model.NewSimpleQuery(model.NewSimpleStatement("error, should be unreachable"), false)
}

// parseDateTimeString returns string used to parse DateTime in Clickhouse (depends on column type)
func (cw *ClickhouseQueryTranslator) parseDateTimeString(table *clickhouse.Table, field, dateTime string) (string, string) {
	typ := table.GetDateTimeType(cw.Ctx, field)
	switch typ {
	case clickhouse.DateTime64:
		return "parseDateTime64BestEffort('" + dateTime + "')", "parseDateTime64BestEffort"
	case clickhouse.DateTime:
		return "parseDateTimeBestEffort('" + dateTime + "')", "parseDateTimeBestEffort"
	default:
		logger.Error().Msgf("invalid DateTime type: %T for field: %s, parsed dateTime value: %s", typ, field, dateTime)
		return "", ""
	}
}

// TODO: not supported:
// - The field has "index" : false and "doc_values" : false set in the mapping
// - The length of the field value exceeded an ignore_above setting in the mapping
// - The field value was malformed and ignore_malformed was defined in the mapping
func (cw *ClickhouseQueryTranslator) parseExists(queryMap QueryMap) model.SimpleQuery {
	sql := model.NewSimpleStatement("")
	for _, v := range queryMap {
		fieldName, ok := v.(string)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid exists type: %T, value: %v", v, v)
			return model.NewSimpleQuery(model.NewSimpleStatement("invalid exists type"), false)
		}
		fieldName = cw.Table.ResolveField(cw.Ctx, fieldName)
		fieldNameQuoted := strconv.Quote(fieldName)

		switch cw.Table.GetFieldInfo(cw.Ctx, fieldName) {
		case clickhouse.ExistsAndIsBaseType:
			simpleStatement := model.NewSimpleStatement(fieldNameQuoted + " IS NOT NULL")
			simpleStatement.WhereStatement = wc.NewInfixOp(wc.NewColumnRef(fieldNameQuoted), "IS", wc.NewLiteral("NOT NULL"))
			statement := simpleStatement
			sql = statement
		case clickhouse.ExistsAndIsArray:
			statement := model.NewSimpleStatement(fieldNameQuoted + ".size0 = 0")
			statement.WhereStatement = wc.NewInfixOp(wc.NewNestedProperty(
				*wc.NewColumnRef(fieldNameQuoted),
				*wc.NewLiteral("size0"),
			), "=", wc.NewLiteral("0"))
			sql = statement
		case clickhouse.NotExists:
			attrs := cw.Table.GetAttributesList()
			stmts := make([]model.Statement, len(attrs))
			for i, a := range attrs {
				compoundStatementNoFieldName := model.NewCompoundStatementNoFieldName(
					fmt.Sprintf("has(%s,%s) AND %s[indexOf(%s,%s)] IS NOT NULL",
						strconv.Quote(a.KeysArrayName), fieldNameQuoted, strconv.Quote(a.ValuesArrayName),
						strconv.Quote(a.KeysArrayName), fieldNameQuoted,
					),
				)
				compoundStatementNoFieldName.WhereStatement = nil
				hasFunc := wc.NewFunction("has", []wc.Statement{wc.NewColumnRef(a.KeysArrayName), wc.NewColumnRef(fieldName)}...)
				arrayAccess := wc.NewArrayAccess(*wc.NewColumnRef(a.ValuesArrayName), wc.NewFunction("indexOf", []wc.Statement{wc.NewColumnRef(a.KeysArrayName), wc.NewLiteral(fieldNameQuoted)}...))
				isNotNull := wc.NewInfixOp(arrayAccess, "IS", wc.NewLiteral("NOT NULL"))
				compoundStatementNoFieldName.WhereStatement = wc.NewInfixOp(hasFunc, "AND", isNotNull)
				stmts[i] = compoundStatementNoFieldName
			}
			sql = model.Or(stmts)
		default:
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid field type: %T for exists: %s", cw.Table.GetFieldInfo(cw.Ctx, fieldName), fieldName)
		}
	}
	return model.NewSimpleQuery(sql, true)
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
			return cw.Table.GetFulltextFields()
		}
		fieldStr = cw.Table.ResolveField(cw.Ctx, fieldStr)
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
// - facets: (Facets, field name, nrOfGroupedBy, sampleSize)
// - listByField: (ListByField, field name, 0, LIMIT)
// - listAllFields: (ListAllFields, "*", 0, LIMIT) (LIMIT = how many rows we want to return)
func (cw *ClickhouseQueryTranslator) tryProcessSearchMetadata(queryMap QueryMap) model.SearchQueryInfo {
	metadata := cw.parseMetadata(queryMap) // TODO we can remove this if we need more speed. It's a bit unnecessary call, at least for now, when we're parsing brutally.

	// case 1: maybe it's a Facets request
	if queryInfo, ok := cw.isItFacetsRequest(metadata); ok {
		return queryInfo
	}

	// case 2: maybe it's ListByField ListAllFields request
	if queryInfo, ok := cw.isItListRequest(metadata); ok {
		return queryInfo
	}

	// case 3: maybe it's a normal request
	var queryMapNested QueryMap
	var ok bool
	size := cw.parseSize(metadata, model.DefaultSizeListQuery)
	if queryMapNested, ok = queryMap["aggs"].(QueryMap); !ok {
		return model.SearchQueryInfo{Typ: model.Normal, I2: size}
	}
	if queryMapNested, ok = queryMapNested["suggestions"].(QueryMap); !ok {
		return model.SearchQueryInfo{Typ: model.Normal, I2: size}
	}
	if queryMapNested, ok = queryMapNested["terms"].(QueryMap); !ok {
		return model.SearchQueryInfo{Typ: model.Normal, I2: size}
	}
	if _, ok = queryMapNested["field"]; !ok {
		return model.SearchQueryInfo{Typ: model.Normal, I2: size}
	}

	// otherwise: None
	return model.NewSearchQueryInfoNone()
}

// 'queryMap' - metadata part of the JSON query
// returns (info, true) if metadata shows it's Facets request
// returns (model.NewSearchQueryInfoNone, false) if it's not Facets request
func (cw *ClickhouseQueryTranslator) isItFacetsRequest(queryMap QueryMap) (model.SearchQueryInfo, bool) {
	queryMap, ok := queryMap["aggs"].(QueryMap)
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}
	queryMap, ok = queryMap["sample"].(QueryMap)
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}
	aggs, ok := queryMap["aggs"].(QueryMap)
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}

	aggsNr := len(aggs)
	// simple "facets" aggregation, which we try to match here, will have here:
	// * "top_values" and "sample_count" keys
	// * aggsNr = 2 (or 4 and 'max_value', 'min_value', as remaining 2)
	_, ok = aggs["sample_count"]
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}
	firstNestingMap, ok := aggs["top_values"].(QueryMap)
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}

	firstNestingMap, ok = firstNestingMap["terms"].(QueryMap)
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}

	size, ok := cw.parseSizeExists(firstNestingMap)
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}
	fieldNameRaw, ok := firstNestingMap["field"]
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}
	fieldName, ok := fieldNameRaw.(string)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("invalid field type: %T, value: %v. Expected string", fieldNameRaw, fieldNameRaw)
		return model.NewSearchQueryInfoNone(), false
	}
	fieldName = strings.TrimSuffix(fieldName, ".keyword")
	fieldName = cw.Table.ResolveField(cw.Ctx, fieldName)

	secondNestingMap, ok := queryMap["sampler"].(QueryMap)
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}
	shardSize, ok := secondNestingMap["shard_size"].(float64)
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}

	if aggsNr == 2 {
		// normal facets
		return model.SearchQueryInfo{Typ: model.Facets, FieldName: fieldName, I1: size, I2: int(shardSize)}, true
	} else if aggsNr == 4 {
		// maybe numeric facets
		_, minExists := aggs["min_value"]
		_, maxExists := aggs["max_value"]
		if minExists && maxExists {
			return model.SearchQueryInfo{Typ: model.FacetsNumeric, FieldName: fieldName, I1: size, I2: int(shardSize)}, true
		}
	}
	return model.NewSearchQueryInfoNone(), false
}

// 'queryMap' - metadata part of the JSON query
// returns (info, true) if metadata shows it's ListAllFields or ListByField request (used e.g. for listing all rows in Kibana)
// returns (model.NewSearchQueryInfoNone, false) if it's not ListAllFields/ListByField request
func (cw *ClickhouseQueryTranslator) isItListRequest(queryMap QueryMap) (model.SearchQueryInfo, bool) {
	// 1) case: very simple SELECT * kind of request
	size, ok := cw.parseSizeExists(queryMap)
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}

	_, okTrackTotalHits := queryMap["track_total_hits"]
	if okTrackTotalHits && len(queryMap) == 2 {
		// only ["size"] and ["track_total_hits"] are present
		return model.SearchQueryInfo{Typ: model.ListAllFields, RequestedFields: []string{"*"}, FieldName: "*", I1: 0, I2: size}, true
	}

	// 2) more general case:
	fields, ok := queryMap["fields"].([]any)
	if !ok {
		return model.NewSearchQueryInfoNone(), false
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
				return model.NewSearchQueryInfoNone(), false
			}
		}
		logger.Debug().Msgf("requested more than one field %s, falling back to '*'", fieldNames)
		// so far everywhere I've seen, > 1 field ==> "*" is one of them
		return model.SearchQueryInfo{Typ: model.ListAllFields, RequestedFields: []string{"*"}, FieldName: "*", I1: 0, I2: size}, true
	} else if len(fields) == 0 {
		isCount, ok := queryMap["track_total_hits"].(bool)
		if ok && isCount {
			return model.SearchQueryInfo{Typ: model.CountAsync, RequestedFields: make([]string, 0), FieldName: "", I1: 0, I2: 0}, true
		}
		return model.NewSearchQueryInfoNone(), false
	} else {
		// 2 cases are possible:
		// a) just a string
		fieldName, ok := fields[0].(string)
		if !ok {
			queryMap, ok = fields[0].(QueryMap)
			if !ok {
				return model.NewSearchQueryInfoNone(), false
			}
			// b) {"field": fieldName}
			if field, ok := queryMap["field"]; ok {
				if fieldName, ok = field.(string); !ok {
					logger.WarnWithCtx(cw.Ctx).Msgf("invalid field type: %T, value: %v. Expected string", field, field)
					return model.NewSearchQueryInfoNone(), false
				}
			} else {
				return model.NewSearchQueryInfoNone(), false
			}
		}

		resolvedField := cw.Table.ResolveField(cw.Ctx, fieldName)
		if resolvedField == "*" {
			return model.SearchQueryInfo{Typ: model.ListAllFields, RequestedFields: []string{"*"}, FieldName: "*", I1: 0, I2: size}, true
		}
		return model.SearchQueryInfo{Typ: model.ListByField, RequestedFields: []string{resolvedField}, FieldName: resolvedField, I1: 0, I2: size}, true
	}
}

func (cw *ClickhouseQueryTranslator) extractInterval(queryMap QueryMap) string {
	const defaultInterval = "30s"
	if fixedInterval, exists := queryMap["fixed_interval"]; exists {
		if asString, ok := fixedInterval.(string); ok {
			return asString
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("unexpected type of interval: %T, value: %v. Returning default", fixedInterval, fixedInterval)
			return defaultInterval
		}
	}
	if calendarInterval, exists := queryMap["calendar_interval"]; exists {
		if asString, ok := calendarInterval.(string); ok {
			return asString
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("unexpected type of interval: %T, value: %v. Returning default", calendarInterval, calendarInterval)
			return defaultInterval
		}
	}

	logger.WarnWithCtx(cw.Ctx).Msgf("extractInterval: no interval found, returning default: %s", defaultInterval)
	return defaultInterval
}

// parseSortFields parses sort fields from the query
// We're skipping ELK internal fields, like "_doc", "_id", etc. (we only accept field starting with "_" if it exists in our table)
func (cw *ClickhouseQueryTranslator) parseSortFields(sortMaps any) (sortFields []model.SortField) {
	sortFields = make([]model.SortField, 0)
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
				if strings.HasPrefix(k, "_") && cw.Table.GetFieldInfo(cw.Ctx, k) == clickhouse.NotExists {
					// we're skipping ELK internal fields, like "_doc", "_id", etc.
					continue
				}
				fieldName := cw.Table.ResolveField(cw.Ctx, k)
				switch v := v.(type) {
				case QueryMap:
					if order, ok := v["order"]; ok {
						if orderAsString, ok := order.(string); ok {
							orderAsString = strings.ToLower(orderAsString)
							if orderAsString == "asc" || orderAsString == "desc" {
								sortFields = append(sortFields, model.SortField{Field: fieldName, Desc: orderAsString == "desc"})
							} else {
								logger.WarnWithCtx(cw.Ctx).Msgf("unexpected order value: %s. Skipping", orderAsString)
							}
						} else {
							logger.WarnWithCtx(cw.Ctx).Msgf("unexpected order type: %T, value: %v. Skipping", order, order)
						}
					} else {
						sortFields = append(sortFields, model.SortField{Field: fieldName, Desc: false})
					}
				case string:
					v = strings.ToLower(v)
					if v == "asc" || v == "desc" {
						sortFields = append(sortFields, model.SortField{Field: fieldName, Desc: v == "desc"})
					} else {
						logger.WarnWithCtx(cw.Ctx).Msgf("unexpected order value: %s. Skipping", v)
					}
				default:
					logger.WarnWithCtx(cw.Ctx).Msgf("unexpected 'sort' value's type: %T (key, value): (%s, %v). Skipping", v, k, v)
				}
			}
		}
		return sortFields
	case map[string]interface{}:
		for fieldName, fieldValue := range sortMaps {
			if strings.HasPrefix(fieldName, "_") && cw.Table.GetFieldInfo(cw.Ctx, fieldName) == clickhouse.NotExists {
				// TODO Elastic internal fields will need to be supported in the future
				continue
			}
			if fieldValue, ok := fieldValue.(string); ok {
				fieldValue = strings.ToLower(fieldValue)
				if fieldValue == "asc" || fieldValue == "desc" {
					sortFields = append(sortFields, model.SortField{Field: fieldName, Desc: fieldValue == "desc"})
				} else {
					logger.WarnWithCtx(cw.Ctx).Msgf("unexpected order value: %s. Skipping", fieldValue)
				}
			}
		}

		return sortFields

	case map[string]string:
		for fieldName, fieldValue := range sortMaps {
			if strings.HasPrefix(fieldName, "_") && cw.Table.GetFieldInfo(cw.Ctx, fieldName) == clickhouse.NotExists {
				// TODO Elastic internal fields will need to be supported in the future
				continue
			}
			fieldValue = strings.ToLower(fieldValue)
			if fieldValue == "asc" || fieldValue == "desc" {
				sortFields = append(sortFields, model.SortField{Field: fieldName, Desc: fieldValue == "desc"})
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("unexpected order value: %s. Skipping", fieldValue)
			}
		}

		return sortFields
	default:
		logger.ErrorWithCtx(cw.Ctx).Msgf("unexpected type of sortMaps: %T, value: %v", sortMaps, sortMaps)
		return []model.SortField{}
	}
}

func (cw *ClickhouseQueryTranslator) parseSizeExists(queryMap QueryMap) (size int, ok bool) {
	sizeRaw, exists := queryMap["size"]
	if !exists {
		return model.DefaultSizeListQuery, false
	} else if sizeAsFloat, ok := sizeRaw.(float64); ok {
		return int(sizeAsFloat), true
	} else {
		logger.WarnWithCtx(cw.Ctx).Msgf("invalid size type: %T, value: %v. Expected float64", sizeRaw, sizeRaw)
		return model.DefaultSizeListQuery, false
	}
}

func (cw *ClickhouseQueryTranslator) parseSize(queryMap QueryMap, defaultSize int) int {
	sizeRaw, exists := queryMap["size"]
	if !exists {
		return defaultSize
	} else if sizeAsFloat, ok := sizeRaw.(float64); ok {
		return int(sizeAsFloat)
	} else {
		logger.WarnWithCtx(cw.Ctx).Msgf("invalid size type: %T, value: %v. Expected float64", sizeRaw, sizeRaw)
		return defaultSize
	}
}
