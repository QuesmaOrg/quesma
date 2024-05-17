package queryparser

import (
	"encoding/json"

	"fmt"
	"github.com/k0kubun/pp"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/queryparser/lucene"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type QueryMap = map[string]interface{}

type SimpleQuery struct {
	Sql        Statement
	CanParse   bool
	FieldName  string
	SortFields []string
}

type Statement struct {
	Stmt       string
	isCompound bool // "a" -> not compound, "a AND b" -> compound. Used to not make unnecessary brackets (not always, but usually)
	FieldName  string
}

// Added to the generated SQL where the query is fine, but we're sure no rows will match it
var alwaysFalseStatement = NewSimpleStatement("false")

// NewEmptyHighlighter returns no-op for error branches and tests
func NewEmptyHighlighter() model.Highlighter {
	return model.Highlighter{
		Fields: make(map[string]bool),
	}
}

func newSimpleQuery(sql Statement, canParse bool) SimpleQuery {
	return SimpleQuery{Sql: sql, CanParse: canParse}
}

func newSimpleQueryWithFieldName(sql Statement, canParse bool, fieldName string) SimpleQuery {
	return SimpleQuery{Sql: sql, CanParse: canParse, FieldName: fieldName}
}

func NewSimpleStatement(stmt string) Statement {
	return Statement{Stmt: stmt, isCompound: false}
}

func NewCompoundStatement(stmt, fieldName string) Statement {
	return Statement{Stmt: stmt, isCompound: true, FieldName: fieldName}
}

func NewCompoundStatementNoFieldName(stmt string) Statement {
	return Statement{Stmt: stmt, isCompound: true}
}

func (cw *ClickhouseQueryTranslator) ParseQuery(queryAsJson string) (SimpleQuery, model.SearchQueryInfo, model.Highlighter, error) {
	cw.ClearTokensToHighlight()
	queryAsMap := make(QueryMap)
	if queryAsJson != "" {
		err := json.Unmarshal([]byte(queryAsJson), &queryAsMap)
		if err != nil {
			logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("error parsing query request's JSON")
			return SimpleQuery{}, model.SearchQueryInfo{}, NewEmptyHighlighter(), err
		}
	}

	// we must parse "highlights" here, because it is stripped from the queryAsMap later
	highlighter := cw.ParseHighlighter(queryAsMap)

	var parsedQuery SimpleQuery
	if queryPart, ok := queryAsMap["query"]; ok {
		parsedQuery = cw.parseQueryMap(queryPart.(QueryMap))
	} else {
		parsedQuery = newSimpleQuery(NewSimpleStatement(""), true)
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

func (cw *ClickhouseQueryTranslator) ParseQueryAsyncSearch(queryAsJson string) (SimpleQuery, model.SearchQueryInfo, model.Highlighter) {
	cw.ClearTokensToHighlight()
	queryAsMap := make(QueryMap)
	err := json.Unmarshal([]byte(queryAsJson), &queryAsMap)
	if err != nil {
		logger.ErrorWithCtx(cw.Ctx).Err(err).Msg("error parsing query request's JSON")
		return newSimpleQuery(NewSimpleStatement("invalid JSON (ParseQueryAsyncSearch)"), false), model.NewSearchQueryInfoNone(), NewEmptyHighlighter()
	}

	// we must parse "highlights" here, because it is stripped from the queryAsMap later
	highlighter := cw.ParseHighlighter(queryAsMap)

	var parsedQuery SimpleQuery
	if query, ok := queryAsMap["query"]; ok {
		queryMap, ok := query.(QueryMap)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid query type: %T, value: %v", query, query)
			return newSimpleQuery(NewSimpleStatement("invalid query type"), false), model.NewSearchQueryInfoNone(), NewEmptyHighlighter()
		}
		parsedQuery = cw.parseQueryMap(queryMap)
	} else {
		return newSimpleQuery(NewSimpleStatement(""), true), cw.tryProcessSearchMetadata(queryAsMap), highlighter
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

func (cw *ClickhouseQueryTranslator) ParseAutocomplete(indexFilter *QueryMap, fieldName string, prefix *string, caseIns bool) SimpleQuery {
	fieldName = cw.Table.ResolveField(cw.Ctx, fieldName)
	canParse := true
	stmts := make([]Statement, 0)
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
		stmts = append(stmts, NewSimpleStatement(fieldName+" "+like+" '"+*prefix+"%'"))
	}
	return newSimpleQuery(and(stmts), canParse)
}

func (cw *ClickhouseQueryTranslator) parseQueryMap(queryMap QueryMap) SimpleQuery {
	if len(queryMap) != 1 {
		// TODO suppress metadata for now
		_ = cw.parseMetadata(queryMap)
	}
	parseMap := map[string]func(QueryMap) SimpleQuery{
		"match_all":           cw.parseMatchAll,
		"match":               func(qm QueryMap) SimpleQuery { return cw.parseMatch(qm, false) },
		"multi_match":         cw.parseMultiMatch,
		"bool":                cw.parseBool,
		"term":                cw.parseTerm,
		"terms":               cw.parseTerms,
		"query":               cw.parseQueryMap,
		"prefix":              cw.parsePrefix,
		"nested":              cw.parseNested,
		"match_phrase":        func(qm QueryMap) SimpleQuery { return cw.parseMatch(qm, true) },
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
		return newSimpleQuery(NewSimpleStatement(""), true)
	}

	// if we can't parse the query, we should show the bug
	unparsedQuery := pp.Sprint(queryMap)
	if prettyMarshal, err := json.Marshal(queryMap); err == nil {
		unparsedQuery = string(prettyMarshal)
	}
	return newSimpleQuery(NewSimpleStatement("can't parse query: "+unparsedQuery), false)
}

// `constant_score` query is just a wrapper for filter query which returns constant relevance score, which we ignore anyway
func (cw *ClickhouseQueryTranslator) parseConstantScore(queryMap QueryMap) SimpleQuery {
	if _, ok := queryMap["filter"]; ok {
		return cw.parseBool(queryMap)
	} else {
		return newSimpleQuery(NewSimpleStatement("parsing error: `constant_score` needs to wrap `filter` query"), false)
	}
}

func (cw *ClickhouseQueryTranslator) parseIds(queryMap QueryMap) SimpleQuery {
	var ids []string
	if val, ok := queryMap["values"]; ok {
		if values, ok := val.([]interface{}); ok {
			for _, id := range values {
				ids = append(ids, id.(string))
			}
		}
	} else {
		return newSimpleQuery(NewSimpleStatement("parsing error: missing mandatory `values` field"), false)
	}
	logger.Warn().Msgf("unsupported id query executed, requested ids of [%s]", strings.Join(ids, "','"))

	timestampColumnName, err := cw.GetTimestampFieldName()
	if err != nil {
		logger.Warn().Msgf("id query executed, but not timestamp field configured")
		return newSimpleQuery(NewSimpleStatement(""), true)
	}

	// when our generated ID appears in query looks like this: `18f7b8800b8q1`
	// therefore we need to strip the hex part (before `q`) and convert it to decimal
	// then we can query at DB level
	for i, id := range ids {
		idInHex := strings.Split(id, "q")[0]
		if decimalValue, err := strconv.ParseUint(idInHex, 16, 64); err != nil {
			logger.Error().Msgf("error parsing document id %s: %v", id, err)
			return newSimpleQuery(NewSimpleStatement(""), true)
		} else {
			ids[i] = fmt.Sprintf("%d", decimalValue)
		}
	}

	var statement string
	if v, ok := cw.Table.Cols[timestampColumnName]; ok {
		switch v.Type.String() {
		case clickhouse.DateTime64.String():
			statement = fmt.Sprintf("toUnixTimestamp64Milli(%s) IN (%s) ", strconv.Quote(timestampColumnName), ids)
		case clickhouse.DateTime.String():
			statement = fmt.Sprintf("toUnixTimestamp(%s) *1000 IN (%s) ", strconv.Quote(timestampColumnName), ids)
		default:
			logger.Warn().Msgf("timestamp field of unsupported type %s", v.Type.String())
			return newSimpleQuery(NewSimpleStatement(""), true)
		}
	}
	return newSimpleQuery(NewSimpleStatement(statement), true)
}

// Parses each SimpleQuery separately, returns list of translated SQLs
func (cw *ClickhouseQueryTranslator) parseQueryMapArray(queryMaps []interface{}) (stmts []Statement, canParse bool) {
	stmts = make([]Statement, len(queryMaps))
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

func (cw *ClickhouseQueryTranslator) iterateListOrDictAndParse(queryMaps interface{}) (stmts []Statement, canParse bool) {
	switch queryMapsTyped := queryMaps.(type) {
	case []interface{}:
		return cw.parseQueryMapArray(queryMapsTyped)
	case QueryMap:
		simpleQuery := cw.parseQueryMap(queryMapsTyped)
		return []Statement{simpleQuery.Sql}, simpleQuery.CanParse
	default:
		logger.WarnWithCtx(cw.Ctx).Msgf("Invalid query type: %T, value: %v", queryMapsTyped, queryMapsTyped)
		return []Statement{NewSimpleStatement("invalid iteration")}, false
	}
}

// TODO: minimum_should_match parameter. Now only ints supported and >1 changed into 1
func (cw *ClickhouseQueryTranslator) parseBool(queryMap QueryMap) SimpleQuery {
	var andStmts []Statement
	canParse := true // will stay true only if all subqueries can be parsed
	for _, andPhrase := range []string{"must", "filter"} {
		if queries, ok := queryMap[andPhrase]; ok {
			newAndStmts, canParseThis := cw.iterateListOrDictAndParse(queries)
			andStmts = append(andStmts, newAndStmts...)
			canParse = canParse && canParseThis
		}
	}
	sql := and(andStmts)

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
		orSql := or(orSqls)
		canParse = canParse && canParseThis
		if len(andStmts) == 0 {
			sql = orSql
		} else if len(orSql.Stmt) > 0 {
			sql = and([]Statement{sql, orSql})
		}
	}

	if queries, ok := queryMap["must_not"]; ok {
		sqlNots, canParseThis := cw.iterateListOrDictAndParse(queries)
		sqlNots = filterNonEmpty(sqlNots)
		canParse = canParse && canParseThis
		if len(sqlNots) > 0 {
			orSql := or(sqlNots)
			if orSql.isCompound {
				orSql.Stmt = "NOT (" + orSql.Stmt + ")"
				orSql.isCompound = false // NOT (compound) is again simple
			} else {
				orSql.Stmt = "NOT " + orSql.Stmt
			}
			sql = and([]Statement{sql, orSql})
		}
	}
	return newSimpleQueryWithFieldName(sql, canParse, sql.FieldName)
}

func (cw *ClickhouseQueryTranslator) parseTerm(queryMap QueryMap) SimpleQuery {
	if len(queryMap) == 1 {
		for k, v := range queryMap {
			cw.AddTokenToHighlight(v)
			if k == "_index" { // index is a table name, already taken from URI and moved to FROM clause
				logger.Warn().Msgf("term %s=%v in query body, ignoring in result SQL", k, v)
				return newSimpleQuery(NewSimpleStatement(" 0=0 /* "+strconv.Quote(k)+"="+sprint(v)+" */ "), true)
			}
			return newSimpleQuery(NewSimpleStatement(strconv.Quote(k)+"="+sprint(v)), true)
		}
	}
	logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 term, got: %d. value: %v", len(queryMap), queryMap)
	return newSimpleQuery(NewSimpleStatement("invalid term len, != 1"), false)
}

// TODO remove optional parameters like boost
func (cw *ClickhouseQueryTranslator) parseTerms(queryMap QueryMap) SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 term, got: %d. value: %v", len(queryMap), queryMap)
		return newSimpleQuery(NewSimpleStatement("invalid terms len, != 1"), false)
	}

	for k, v := range queryMap {
		if strings.HasPrefix(k, "_") {
			// terms enum API uses _tier terms ( data_hot, data_warm, etc.)
			// we don't want these internal fields to percolate to the SQL query
			return newSimpleQuery(NewSimpleStatement(""), true)
		}
		vAsArray, ok := v.([]interface{})
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid terms type: %T, value: %v", v, v)
			return newSimpleQuery(NewSimpleStatement("invalid terms type"), false)
		}
		orStmts := make([]Statement, len(vAsArray))
		for i, v := range vAsArray {
			cw.AddTokenToHighlight(v)
			orStmts[i] = NewSimpleStatement(strconv.Quote(k) + "=" + sprint(v))
		}
		return newSimpleQuery(or(orStmts), true)
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return newSimpleQuery(NewSimpleStatement("error, should be unreachable"), false)
}

func (cw *ClickhouseQueryTranslator) parseMatchAll(_ QueryMap) SimpleQuery {
	return newSimpleQuery(NewSimpleStatement(""), true)
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
func (cw *ClickhouseQueryTranslator) parseMatch(queryMap QueryMap, matchPhrase bool) SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 match, got: %d. value: %v", len(queryMap), queryMap)
		return newSimpleQuery(NewSimpleStatement("unsupported match len != 1"), false)
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
			statements := make([]Statement, 0, len(subQueries))
			cw.AddTokenToHighlight(vAsString)
			for _, subQuery := range subQueries {
				cw.AddTokenToHighlight(subQuery)
				if fieldName == "_id" { // We compute this field on the fly using our custom logic, so we have to parse it differently
					computedIdMatchingQuery := cw.parseIds(QueryMap{"values": []interface{}{subQuery}})
					statements = append(statements, computedIdMatchingQuery.Sql)
				} else {
					statements = append(statements, NewSimpleStatement(strconv.Quote(fieldName)+" iLIKE "+"'%"+subQuery+"%'"))
				}
			}
			return newSimpleQuery(or(statements), true)
		}

		cw.AddTokenToHighlight(vUnNested)

		// so far we assume that only strings can be ORed here
		return newSimpleQuery(NewSimpleStatement(strconv.Quote(fieldName)+" == "+sprint(vUnNested)), true)
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return newSimpleQuery(NewSimpleStatement("error, should be unreachable"), false)
}

func (cw *ClickhouseQueryTranslator) parseMultiMatch(queryMap QueryMap) SimpleQuery {
	var fields []string
	fieldsAsInterface, ok := queryMap["fields"]
	if ok {
		if fieldsAsArray, ok := fieldsAsInterface.([]interface{}); ok {
			fields = cw.extractFields(fieldsAsArray)
		} else {
			logger.ErrorWithCtx(cw.Ctx).Msgf("invalid fields type: %T, value: %v", fieldsAsInterface, fieldsAsInterface)
			return newSimpleQuery(NewSimpleStatement("invalid fields type"), false)
		}
	} else {
		fields = cw.Table.GetFields()
	}
	if len(fields) == 0 {
		return newSimpleQuery(alwaysFalseStatement, true)
	}

	query, ok := queryMap["query"]
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("no query in multi_match query: %v", queryMap)
		return newSimpleQuery(alwaysFalseStatement, false)
	}
	queryAsString, ok := query.(string)
	if !ok {
		logger.WarnWithCtx(cw.Ctx).Msgf("invalid query type: %T, value: %v", query, query)
		return newSimpleQuery(alwaysFalseStatement, false)
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

	sqls := make([]Statement, len(fields)*len(subQueries))
	i := 0
	for _, field := range fields {
		for _, subQ := range subQueries {
			sqls[i] = NewSimpleStatement(strconv.Quote(field) + " iLIKE '%" + subQ + "%'")
			i++
		}
	}
	return newSimpleQuery(or(sqls), true)
}

// prefix works only on strings
func (cw *ClickhouseQueryTranslator) parsePrefix(queryMap QueryMap) SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 prefix, got: %d. value: %v", len(queryMap), queryMap)
		return newSimpleQuery(NewSimpleStatement("invalid prefix len != 1"), false)
	}

	for fieldName, v := range queryMap {
		fieldName = cw.Table.ResolveField(cw.Ctx, fieldName)
		switch vCasted := v.(type) {
		case string:
			cw.AddTokenToHighlight(vCasted)
			return newSimpleQuery(NewSimpleStatement(strconv.Quote(fieldName)+" iLIKE '"+vCasted+"%'"), true)
		case QueryMap:
			token := vCasted["value"].(string)
			cw.AddTokenToHighlight(token)
			return newSimpleQuery(NewSimpleStatement(strconv.Quote(fieldName)+" iLIKE '"+token+"%'"), true)
		default:
			logger.WarnWithCtx(cw.Ctx).Msgf("unsupported prefix type: %T, value: %v", v, v)
			return newSimpleQuery(NewSimpleStatement("unsupported prefix type"), false)
		}
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return newSimpleQuery(NewSimpleStatement("error, should be unreachable"), false)
}

// Not supporting 'case_insensitive' (optional)
// Also not supporting wildcard (Required, string) (??) In both our example, and their in docs,
// it's not provided.
func (cw *ClickhouseQueryTranslator) parseWildcard(queryMap QueryMap) SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 wildcard, got: %d. value: %v", len(queryMap), queryMap)
		return newSimpleQuery(NewSimpleStatement("invalid wildcard len != 1"), false)
	}

	for fieldName, v := range queryMap {
		fieldName = cw.Table.ResolveField(cw.Ctx, fieldName)
		if vAsMap, ok := v.(QueryMap); ok {
			if value, ok := vAsMap["value"]; ok {
				if valueAsString, ok := value.(string); ok {
					cw.AddTokenToHighlight(valueAsString)
					return newSimpleQuery(NewSimpleStatement(strconv.Quote(fieldName)+" iLIKE '"+
						strings.ReplaceAll(valueAsString, "*", "%")+"'"), true)
				} else {
					logger.WarnWithCtx(cw.Ctx).Msgf("invalid value type: %T, value: %v", value, value)
					return newSimpleQuery(NewSimpleStatement("invalid value type"), false)
				}
			} else {
				logger.WarnWithCtx(cw.Ctx).Msgf("no value in wildcard query: %v", queryMap)
				return newSimpleQuery(NewSimpleStatement("no value in wildcard query"), false)
			}
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid wildcard type: %T, value: %v", v, v)
			return newSimpleQuery(NewSimpleStatement("invalid wildcard type"), false)
		}
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return newSimpleQuery(NewSimpleStatement("error, should be unreachable"), false)
}

// This one is really complicated (https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html)
// `query` uses Lucene language, we don't support 100% of it, but most.
func (cw *ClickhouseQueryTranslator) parseQueryString(queryMap QueryMap) SimpleQuery {
	var fields []string
	if fieldsRaw, ok := queryMap["fields"]; ok {
		fields = cw.extractFields(fieldsRaw.([]interface{}))
	} else {
		fields = cw.Table.GetFields()
	}

	query := queryMap["query"].(string) // query: (Required, string)

	// TODO This highlighting seems not that bad for the first version,
	// but we probably should improve it, at least a bit
	cw.AddTokenToHighlight(query)
	for _, querySubstring := range strings.Split(query, " ") {
		cw.AddTokenToHighlight(querySubstring)
	}

	// we always can parse, with invalid query we return "false"
	return newSimpleQuery(NewSimpleStatement(lucene.TranslateToSQL(cw.Ctx, query, fields)), true)
}

func (cw *ClickhouseQueryTranslator) parseNested(queryMap QueryMap) SimpleQuery {
	if query, ok := queryMap["query"]; ok {
		if queryAsMap, ok := query.(QueryMap); ok {
			return cw.parseQueryMap(queryAsMap)
		} else {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid nested query type: %T, value: %v", query, query)
			return newSimpleQuery(NewSimpleStatement("invalid nested query type"), false)
		}
	}

	logger.WarnWithCtx(cw.Ctx).Msgf("no query in nested query: %v", queryMap)
	return newSimpleQuery(NewSimpleStatement("no query in nested query"), false)
}

func parseDateMathExpression(expr string) (string, error) {
	expr = strings.ReplaceAll(expr, "'", "")

	exp, err := ParseDateMathExpression(expr)
	if err != nil {
		logger.Warn().Msgf("error parsing date math expression: %s", expr)
		return "", err
	}

	builder := &DateMathAsClickhouseIntervals{}

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
func (cw *ClickhouseQueryTranslator) parseRange(queryMap QueryMap) SimpleQuery {
	if len(queryMap) != 1 {
		logger.WarnWithCtx(cw.Ctx).Msgf("we expect only 1 range, got: %d. value: %v", len(queryMap), queryMap)
		return newSimpleQuery(NewSimpleStatement("invalid range len != 1"), false)
	}

	for field, v := range queryMap {
		field = cw.Table.ResolveField(cw.Ctx, field)
		stmts := make([]Statement, 0)
		if _, ok := v.(QueryMap); !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid range type: %T, value: %v", v, v)
			continue
		}
		isDatetimeInDefaultFormat := true // in 99% requests, format is "strict_date_optional_time", which we can parse with time.Parse(time.RFC3339Nano, ..)
		if format, ok := v.(QueryMap)["format"]; ok && format == "epoch_millis" {
			isDatetimeInDefaultFormat = false
		}

		for op, v := range v.(QueryMap) {
			fieldType := cw.Table.GetDateTimeType(cw.Ctx, field)
			vToPrint := sprint(v)
			var fieldToPrint string
			if !isDatetimeInDefaultFormat {
				fieldToPrint = "toUnixTimestamp64Milli(" + strconv.Quote(field) + ")"
			} else {
				fieldToPrint = strconv.Quote(field)
				switch fieldType {
				case clickhouse.DateTime64, clickhouse.DateTime:
					if dateTime, ok := v.(string); ok {
						// if it's a date, we need to parse it to Clickhouse's DateTime format
						// how to check if it does not contain date math expression?
						if _, err := time.Parse(time.RFC3339Nano, dateTime); err == nil {
							vToPrint = cw.parseDateTimeString(cw.Table, field, dateTime)
						} else if op == "gte" || op == "lte" || op == "gt" || op == "lt" {
							vToPrint, err = parseDateMathExpression(vToPrint)
							if err != nil {
								logger.WarnWithCtx(cw.Ctx).Msgf("error parsing date math expression: %s", vToPrint)
								return newSimpleQuery(NewSimpleStatement("error parsing date math expression: "+vToPrint), false)
							}
						}
					} else if v == nil {
						vToPrint = "NULL"
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
					}
				default:
					logger.WarnWithCtx(cw.Ctx).Msgf("invalid DateTime type for field: %s, parsed dateTime value: %s", field, vToPrint)
				}
			}

			switch op {
			case "gte":
				stmts = append(stmts, NewSimpleStatement(fieldToPrint+">="+vToPrint))
			case "lte":
				stmts = append(stmts, NewSimpleStatement(fieldToPrint+"<="+vToPrint))
			case "gt":
				stmts = append(stmts, NewSimpleStatement(fieldToPrint+">"+vToPrint))
			case "lt":
				stmts = append(stmts, NewSimpleStatement(fieldToPrint+"<"+vToPrint))
			case "format":
				// ignored
			default:
				logger.WarnWithCtx(cw.Ctx).Msgf("invalid range operator: %s", op)
			}
		}
		return newSimpleQueryWithFieldName(and(stmts), true, field)
	}

	// unreachable unless something really weird happens
	logger.ErrorWithCtx(cw.Ctx).Msg("theoretically unreachable code")
	return newSimpleQuery(NewSimpleStatement("error, should be unreachable"), false)
}

// parseDateTimeString returns string used to parse DateTime in Clickhouse (depends on column type)
func (cw *ClickhouseQueryTranslator) parseDateTimeString(table *clickhouse.Table, field, dateTime string) string {
	typ := table.GetDateTimeType(cw.Ctx, field)
	switch typ {
	case clickhouse.DateTime64:
		return "parseDateTime64BestEffort('" + dateTime + "')"
	case clickhouse.DateTime:
		return "parseDateTimeBestEffort('" + dateTime + "')"
	default:
		logger.Error().Msgf("invalid DateTime type: %T for field: %s, parsed dateTime value: %s", typ, field, dateTime)
		return ""
	}
}

// TODO: not supported:
// - The field has "index" : false and "doc_values" : false set in the mapping
// - The length of the field value exceeded an ignore_above setting in the mapping
// - The field value was malformed and ignore_malformed was defined in the mapping
func (cw *ClickhouseQueryTranslator) parseExists(queryMap QueryMap) SimpleQuery {
	sql := NewSimpleStatement("")
	for _, v := range queryMap {
		fieldName, ok := v.(string)
		if !ok {
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid exists type: %T, value: %v", v, v)
			return newSimpleQuery(NewSimpleStatement("invalid exists type"), false)
		}
		fieldName = cw.Table.ResolveField(cw.Ctx, fieldName)
		fieldNameQuoted := strconv.Quote(fieldName)

		switch cw.Table.GetFieldInfo(cw.Ctx, fieldName) {
		case clickhouse.ExistsAndIsBaseType:
			sql = NewSimpleStatement(fieldNameQuoted + " IS NOT NULL")
		case clickhouse.ExistsAndIsArray:
			sql = NewSimpleStatement(fieldNameQuoted + ".size0 = 0")
		case clickhouse.NotExists:
			attrs := cw.Table.GetAttributesList()
			stmts := make([]Statement, len(attrs))
			for i, a := range attrs {
				stmts[i] = NewCompoundStatementNoFieldName(
					fmt.Sprintf("has(%s,%s) AND %s[indexOf(%s,%s)] IS NOT NULL",
						strconv.Quote(a.KeysArrayName), fieldNameQuoted, strconv.Quote(a.ValuesArrayName),
						strconv.Quote(a.KeysArrayName), fieldNameQuoted,
					),
				)
			}
			sql = or(stmts)
		default:
			logger.WarnWithCtx(cw.Ctx).Msgf("invalid field type: %T for exists: %s", cw.Table.GetFieldInfo(cw.Ctx, fieldName), fieldName)
		}
	}
	return newSimpleQuery(sql, true)
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
			return cw.Table.GetFields()
		}
		fieldStr = cw.Table.ResolveField(cw.Ctx, fieldStr)
		result = append(result, fieldStr)
	}
	return result
}

// sep = "AND" or "OR"
func combineStatements(stmts []Statement, sep string) Statement {
	stmts = filterNonEmpty(stmts)
	if len(stmts) > 1 {
		stmts = quoteWithBracketsIfCompound(stmts)
		var fieldName string
		sql := ""
		for i, stmt := range stmts {
			sql += stmt.Stmt
			if i < len(stmts)-1 {
				sql += " " + sep + " "
			}
			if stmt.FieldName != "" {
				fieldName = stmt.FieldName
			}
		}
		return NewCompoundStatement(sql, fieldName)
	}
	if len(stmts) == 1 {
		return stmts[0]
	}
	return NewSimpleStatement("")
}

func and(andStmts []Statement) Statement {
	return combineStatements(andStmts, "AND")
}

func or(orStmts []Statement) Statement {
	return combineStatements(orStmts, "OR")
}

func filterNonEmpty(slice []Statement) []Statement {
	i := 0
	for _, el := range slice {
		if len(el.Stmt) > 0 {
			slice[i] = el
			i++
		}
	}
	return slice[:i]
}

// used to combine statements with AND/OR
// [a, b, a AND b] ==> ["a", "b", "(a AND b)"]
func quoteWithBracketsIfCompound(slice []Statement) []Statement {
	for i := range slice {
		if slice[i].isCompound {
			slice[i].Stmt = "(" + slice[i].Stmt + ")"
		}
	}
	return slice
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
	size, _ := cw.parseSize(metadata)
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

	size, ok := cw.parseSize(firstNestingMap)
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
	size, ok := cw.parseSize(queryMap)
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
func (cw *ClickhouseQueryTranslator) parseSortFields(sortMaps any) []string {
	switch sortMaps := sortMaps.(type) {
	case []any:
		sortFields := make([]string, 0)
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
							sortFields = append(sortFields, strconv.Quote(fieldName)+" "+orderAsString)
						} else {
							logger.WarnWithCtx(cw.Ctx).Msgf("unexpected order type: %T, value: %v. Skipping", order, order)
						}
					} else {
						sortFields = append(sortFields, strconv.Quote(fieldName))
					}
				case string:
					sortFields = append(sortFields, strconv.Quote(fieldName)+" "+v)
				default:
					logger.WarnWithCtx(cw.Ctx).Msgf("unexpected 'sort' value's type: %T (key, value): (%s, %v). Skipping", v, k, v)
				}
			}
		}
		return sortFields
	case map[string]interface{}:
		sortFields := make([]string, 0)

		for fieldName, fieldValue := range sortMaps {
			if strings.HasPrefix(fieldName, "_") && cw.Table.GetFieldInfo(cw.Ctx, fieldName) == clickhouse.NotExists {
				// TODO Elastic internal fields will need to be supported in the future
				continue
			}
			if fieldValue, ok := fieldValue.(string); ok {
				sortFields = append(sortFields, fmt.Sprintf("%s %s", strconv.Quote(fieldName), fieldValue))
			}
		}

		return sortFields

	case map[string]string:
		sortFields := make([]string, 0)

		for fieldName, fieldValue := range sortMaps {
			if strings.HasPrefix(fieldName, "_") && cw.Table.GetFieldInfo(cw.Ctx, fieldName) == clickhouse.NotExists {
				// TODO Elastic internal fields will need to be supported in the future
				continue
			}
			sortFields = append(sortFields, fmt.Sprintf("%s %s", strconv.Quote(fieldName), fieldValue))
		}

		return sortFields
	default:
		logger.ErrorWithCtx(cw.Ctx).Msgf("unexpected type of sortMaps: %T, value: %v", sortMaps, sortMaps)
		return []string{}
	}
}

func (cw *ClickhouseQueryTranslator) parseSize(queryMap QueryMap) (size int, ok bool) {
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
