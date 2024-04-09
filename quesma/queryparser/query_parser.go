package queryparser

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/k0kubun/pp"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"sort"
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

type Highlighter struct {
	Tokens []string
	Fields map[string]bool

	PreTags  []string
	PostTags []string
}

// NewEmptyHighlighter returns no-op for error branches and tests
func NewEmptyHighlighter() Highlighter {
	return Highlighter{
		Fields: make(map[string]bool),
	}
}

func (h Highlighter) ShouldHighlight(columnName string) bool {
	_, ok := h.Fields[columnName]
	return ok
}

func (h Highlighter) HighlightValue(value string) []string {

	//https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html
	// https://medium.com/@andre.luiz1987/using-highlighting-elasticsearch-9ccd698f08

	// paranoia check for empty tags
	if len(h.PreTags) < 1 && len(h.PostTags) < 1 {
		return []string{}
	}

	type match struct {
		start int
		end   int
	}

	var matches []match

	lowerValue := strings.ToLower(value)
	length := len(lowerValue)

	// find all matches
	for _, token := range h.Tokens {
		pos := 0
		for pos < length {
			// token are lower cased already
			idx := strings.Index(lowerValue[pos:], token)
			if idx == -1 {
				break
			}

			start := pos + idx
			end := start + len(token)

			matches = append(matches, match{start, end})
			pos = end
		}
	}

	if len(matches) == 0 {
		return []string{}
	}

	// sort matches by start position
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].start < matches[j].start
	})

	var mergedMatches []match

	// merge overlapping matches
	for i := 0; i < len(matches); i++ {
		lastMerged := len(mergedMatches) - 1

		if len(mergedMatches) > 0 && matches[i].start <= mergedMatches[len(mergedMatches)-1].end {
			mergedMatches[lastMerged].end = max(matches[i].end, mergedMatches[lastMerged].end)
		} else {
			mergedMatches = append(mergedMatches, matches[i])
		}

	}

	// populate highlights
	var highlights []string
	for _, m := range mergedMatches {
		highlights = append(highlights, h.PreTags[0]+value[m.start:m.end]+h.PostTags[0])
	}

	return highlights
}

func (h *Highlighter) SetTokens(tokens []string) {

	uniqueTokens := make(map[string]bool)
	for _, token := range tokens {
		uniqueTokens[strings.ToLower(token)] = true
	}

	h.Tokens = make([]string, 0, len(uniqueTokens))
	for token := range uniqueTokens {
		h.Tokens = append(h.Tokens, token)
	}

	// longer tokens firsts
	sort.Slice(h.Tokens, func(i, j int) bool {
		return len(h.Tokens[i]) > len(h.Tokens[j])
	})

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

func (cw *ClickhouseQueryTranslator) ParseQuery(queryAsJson string) (SimpleQuery, model.SearchQueryInfo, Highlighter) {
	cw.ClearTokensToHighlight()
	queryAsMap := make(QueryMap)
	err := json.Unmarshal([]byte(queryAsJson), &queryAsMap)
	if err != nil {
		return newSimpleQuery(NewSimpleStatement("invalid JSON (ParseQuery)"), false), model.NewSearchQueryInfoNone(), NewEmptyHighlighter()
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
		if sortAsArray, ok := sortPart.([]any); ok {
			parsedQuery.SortFields = cw.parseSortFields(sortAsArray)
		}
	}

	const defaultSize = 0
	var size int
	if sizeRaw, ok := queryAsMap["size"]; ok {
		size = int(sizeRaw.(float64))
	} else {
		size = defaultSize
	}

	queryInfo := cw.tryProcessSearchMetadata(queryAsMap)
	queryInfo.Size = size

	highlighter.SetTokens(cw.tokensToHighlight)
	cw.ClearTokensToHighlight()

	return parsedQuery, queryInfo, highlighter
}

func (cw *ClickhouseQueryTranslator) ParseHighlighter(queryMap QueryMap) Highlighter {

	highlight, ok := queryMap["highlight"].(QueryMap)

	// if the kibana is not interested in highlighting, we return dummy object
	if !ok {
		return NewEmptyHighlighter()
	}

	var highlighter Highlighter

	if pre, ok := highlight["pre_tags"]; ok {
		for _, x := range pre.([]interface{}) {
			highlighter.PreTags = append(highlighter.PreTags, x.(string))
		}
	}
	if post, ok := highlight["post_tags"]; ok {
		for _, x := range post.([]interface{}) {
			highlighter.PostTags = append(highlighter.PostTags, x.(string))
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

func (cw *ClickhouseQueryTranslator) ParseQueryAsyncSearch(queryAsJson string) (SimpleQuery, model.SearchQueryInfo, Highlighter) {
	cw.ClearTokensToHighlight()
	queryAsMap := make(QueryMap)
	err := json.Unmarshal([]byte(queryAsJson), &queryAsMap)
	if err != nil {
		return newSimpleQuery(NewSimpleStatement("invalid JSON (parseQueryAsyncSearch)"), false), model.NewSearchQueryInfoNone(), NewEmptyHighlighter()
	}

	// we must parse "highlights" here, because it is stripped from the queryAsMap later
	highlighter := cw.ParseHighlighter(queryAsMap)

	if _, ok := queryAsMap["query"]; !ok {
		return newSimpleQuery(NewSimpleStatement(""), true), cw.tryProcessSearchMetadata(queryAsMap), highlighter
	}

	parsedQuery := cw.parseQueryMap(queryAsMap["query"].(QueryMap))
	if sort, ok := queryAsMap["sort"]; ok {
		if sortAsArray, ok := sort.([]any); ok {
			parsedQuery.SortFields = cw.parseSortFields(sortAsArray)
		}
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
	fieldName = cw.Table.ResolveField(fieldName)
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
		"wildcard":            cw.parseWildcard,
		"query_string":        cw.parseQueryString,
		"simple_query_string": cw.parseQueryString,
	}
	for k, v := range queryMap {
		if f, ok := parseMap[k]; ok {
			return f(v.(QueryMap))
		}
	}
	return newSimpleQuery(NewSimpleStatement("can't parse query: "+pp.Sprint(queryMap)), false)
}

// Parses each SimpleQuery separately, returns list of translated SQLs
func (cw *ClickhouseQueryTranslator) parseQueryMapArray(queryMaps []interface{}) []Statement {
	results := make([]Statement, len(queryMaps))
	for i, v := range queryMaps {
		qmap := cw.parseQueryMap(v.(QueryMap))
		results[i] = qmap.Sql
		results[i].FieldName = qmap.FieldName
	}
	return results
}

func (cw *ClickhouseQueryTranslator) iterateListOrDictAndParse(queryMaps interface{}) []Statement {
	switch queryMapsTyped := queryMaps.(type) {
	case []interface{}:
		return cw.parseQueryMapArray(queryMapsTyped)
	case QueryMap:
		return []Statement{cw.parseQueryMap(queryMapsTyped).Sql}
	default:
		return []Statement{NewSimpleStatement("Invalid iteration")}
	}
}

// TODO: minimum_should_match parameter. Now only ints supported and >1 changed into 1
func (cw *ClickhouseQueryTranslator) parseBool(queryMap QueryMap) SimpleQuery {
	var andStmts []Statement
	for _, andPhrase := range []string{"must", "filter"} {
		if queries, ok := queryMap[andPhrase]; ok {
			andStmts = append(andStmts, cw.iterateListOrDictAndParse(queries)...)
		}
	}
	sql := and(andStmts)

	minimumShouldMatch := 0
	if v, ok := queryMap["minimum_should_match"]; ok {
		minimumShouldMatch = int(v.(float64))
	}
	if len(andStmts) == 0 || minimumShouldMatch > 1 {
		minimumShouldMatch = 1
	}
	if queries, ok := queryMap["should"]; ok && minimumShouldMatch == 1 {
		orSql := or(cw.iterateListOrDictAndParse(queries))
		if len(andStmts) == 0 {
			sql = orSql
		} else if len(orSql.Stmt) > 0 {
			sql = and([]Statement{sql, orSql})
		}
	}

	if queries, ok := queryMap["must_not"]; ok {
		sqlNots := cw.iterateListOrDictAndParse(queries)
		sqlNots = filterNonEmpty(sqlNots)
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
	return newSimpleQueryWithFieldName(sql, true, sql.FieldName)
}

func (cw *ClickhouseQueryTranslator) parseTerm(queryMap QueryMap) SimpleQuery {
	if len(queryMap) == 1 {
		for k, v := range queryMap {
			cw.AddTokenToHighlight(v)
			return newSimpleQuery(NewSimpleStatement(strconv.Quote(k)+"="+sprint(v)), true)
		}
	}
	return newSimpleQuery(NewSimpleStatement("invalid term len, != 1"), false)
}

// TODO remove optional parameters like boost
func (cw *ClickhouseQueryTranslator) parseTerms(queryMap QueryMap) SimpleQuery {
	if len(queryMap) == 1 {
		for k, v := range queryMap {
			if strings.HasPrefix(k, "_") {
				// terms enum API uses _tier terms ( data_hot, data_warm, etc.)
				// we don't want these internal fields to percolate to the SQL query
				return newSimpleQuery(NewSimpleStatement(""), true)
			}
			vAsArray := v.([]interface{})
			orStmts := make([]Statement, len(vAsArray))
			for i, v := range vAsArray {
				cw.AddTokenToHighlight(v)
				orStmts[i] = NewSimpleStatement(strconv.Quote(k) + "=" + sprint(v))
			}
			return newSimpleQuery(or(orStmts), true)
		}
	}
	return newSimpleQuery(NewSimpleStatement("invalid terms len, != 1"), false)
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
	if len(queryMap) == 1 {
		for fieldName, v := range queryMap {
			fieldName = cw.Table.ResolveField(fieldName)
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
					statements = append(statements, NewSimpleStatement(strconv.Quote(fieldName)+" iLIKE "+"'%"+subQuery+"%'"))
				}
				return newSimpleQuery(or(statements), true)
			}

			cw.AddTokenToHighlight(vUnNested)

			// so far we assume that only strings can be ORed here
			return newSimpleQuery(NewSimpleStatement(strconv.Quote(fieldName)+" == "+sprint(vUnNested)), true)
		}
	}
	return newSimpleQuery(NewSimpleStatement("unsupported match len != 1"), false)
}

func (cw *ClickhouseQueryTranslator) parseMultiMatch(queryMap QueryMap) SimpleQuery {
	var fields []string
	fieldsAsInterface, ok := queryMap["fields"]
	if ok {
		fields = cw.extractFields(fieldsAsInterface.([]interface{}))
	} else {
		fields = cw.GetFieldsList()
	}

	var subQueries []string
	// 2 cases:
	if matchType, ok := queryMap["type"]; ok && matchType.(string) == "phrase" {
		// a) "type" == "phrase" -> we need to match full string
		subQueries = []string{queryMap["query"].(string)}
	} else {
		// b) "type" == "best_fields" (or other - we treat it as default) -> we need to match any of the words
		subQueries = strings.Split(queryMap["query"].(string), " ")
	}

	cw.AddTokenToHighlight(queryMap["query"].(string))
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
	if len(queryMap) == 1 {
		for fieldName, v := range queryMap {
			fieldName = cw.Table.ResolveField(fieldName)
			switch vCasted := v.(type) {
			case string:
				cw.AddTokenToHighlight(vCasted)
				return newSimpleQuery(NewSimpleStatement(strconv.Quote(fieldName)+" iLIKE '"+vCasted+"%'"), true)
			case QueryMap:
				token := vCasted["value"].(string)
				cw.AddTokenToHighlight(token)
				return newSimpleQuery(NewSimpleStatement(strconv.Quote(fieldName)+" iLIKE '"+token+"%'"), true)
			}
		}
	}
	return newSimpleQuery(NewSimpleStatement("invalid prefix len != 1"), false)
}

// Not supporting 'case_insensitive' (optional)
// Also not supporting wildcard (Required, string) (??) In both our example, and their in docs,
// it's not provided.
func (cw *ClickhouseQueryTranslator) parseWildcard(queryMap QueryMap) SimpleQuery {
	// not checking for len == 1 because it's only option in proper SimpleQuery
	for fieldName, v := range queryMap {
		fieldName = cw.Table.ResolveField(fieldName)
		return newSimpleQuery(NewSimpleStatement(strconv.Quote(fieldName)+" iLIKE '"+strings.ReplaceAll(v.(QueryMap)["value"].(string),
			"*", "%")+"'"), true)
	}
	return newSimpleQuery(NewSimpleStatement("empty wildcard"), false)
}

// This one is REALLY complicated (https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html)
// Supporting 'fields' and 'query' (also, * in 'fields' doesn't support other types than string...)
// + only '*' in query, no '?' or other regex
func (cw *ClickhouseQueryTranslator) parseQueryString(queryMap QueryMap) SimpleQuery {
	orStmts := make([]Statement, 0)
	if fields, ok := queryMap["fields"]; ok {
		fieldsAsStrings := cw.extractFields(fields.([]interface{}))
		for _, field := range fieldsAsStrings {
			query := queryMap["query"].(string)
			cw.AddTokenToHighlight(query)
			for _, qStr := range strings.Split(query, " ") {
				cw.AddTokenToHighlight(qStr)
				orStmts = append(orStmts, NewSimpleStatement(strconv.Quote(field)+" iLIKE '%"+strings.ReplaceAll(qStr, "*", "%")+"%'"))
			}
		}
	} else {
		return cw.parseQueryStringField(queryMap["query"].(string))
	}
	return newSimpleQuery(or(orStmts), true)
}

// TODO it's a very simple implementation. Implement better if needed.
func (cw *ClickhouseQueryTranslator) parseQueryStringField(query string) SimpleQuery {
	split := strings.Split(query, ":")
	if len(split) != 2 {
		return newSimpleQuery(NewSimpleStatement("invalid query string"), false)
	}
	fieldName, value := split[0], split[1]
	fieldName = cw.Table.ResolveField(fieldName)
	if len(value) > 0 && (value[0] == '>' || value[0] == '<') {
		// to support fieldName>value, <value, etc. We see such request in Kibana
		return newSimpleQuery(NewSimpleStatement(fieldName+value), true)
	}
	cw.AddTokenToHighlight(value)
	return newSimpleQuery(NewSimpleStatement(fieldName+" iLIKE '%"+value+"%'"), true)
}

func (cw *ClickhouseQueryTranslator) parseNested(queryMap QueryMap) SimpleQuery {
	return cw.parseQueryMap(queryMap["query"].(QueryMap))
}

func parseTimeUnit(timeUnit string) (string, error) {
	switch timeUnit {
	case "m":
		return "minute", nil
	case "s":
		return "second", nil
	case "h", "H":
		return "hour", nil
	case "d":
		return "day", nil
	case "w":
		return "week", nil
	case "M":
		return "month", nil
	case "y":
		return "year", nil
	}
	return "", errors.New("unsupported time unit")
}

func tokenizeDateMathExpr(expr string) []string {
	tokens := make([]string, 0)
	const NOW_LENGTH = 3
	const OPERATOR_ADD = '+'
	const OPERATOR_SUB = '-'
	for index := 0; index < len(expr); index++ {
		// This is now keyword
		if expr[index] == 'n' {
			if len(expr) < NOW_LENGTH {
				return tokens
			}
			index = index + NOW_LENGTH
			token := expr[:index]
			if token != "now" {
				return tokens
			}
			tokens = append(tokens, token)
		}
		if index < len(expr) && (expr[index] == OPERATOR_ADD || expr[index] == OPERATOR_SUB) {
			token := expr[index : index+1]
			tokens = append(tokens, token)
			index = index + 1
		} else {
			logger.Error().Msgf("operator expected in date math expression '%s'", expr)
			return tokens
		}
		var number string
		for ; index < len(expr)-1; index++ {
			if !unicode.IsDigit(rune(expr[index])) {
				break
			}
			if unicode.IsDigit(rune(expr[index])) {
				number = number + string(expr[index])
			}
		}
		// Check if number has been tokenized
		// correctly and if not, return tokens
		if len(number) == 0 {
			logger.Error().Msgf("number expected in date math expression '%s'", expr)
			return tokens
		}
		tokens = append(tokens, number)
		token := expr[index]
		tokens = append(tokens, string(token))
	}
	return tokens
}

func buildDateMathExpression(tokens []string) string {
	const NEXT_OP_DISTANCE = 3
	const TIME_UNIT_DISTANCE = 2
	const TIME_AMOUNT_DISTANCE = 1
	if len(tokens) == 0 {
		return ""
	}
	tokenIndex := 0
	currentExpr := tokens[tokenIndex]
	switch currentExpr {
	case "now":
		currentExpr = "now()"
	default:
		logger.Error().Msg("unsupported date math argument")
	}
	tokenIndex = tokenIndex + 1
	for tokenIndex+TIME_UNIT_DISTANCE < len(tokens) {
		op := tokens[tokenIndex]
		switch op {
		case "+":
			op = "addDate"
		case "-":
			op = "subDate"
		}
		timeUnit, err := parseTimeUnit(tokens[tokenIndex+TIME_UNIT_DISTANCE])
		if err != nil {
			logger.Error().Msg(err.Error())
			return ""
		}
		timeAmount := tokens[tokenIndex+TIME_AMOUNT_DISTANCE]
		currentExpr = op + "(" + currentExpr + "," + " INTERVAL " + timeAmount + " " + timeUnit + ")"
		tokenIndex = tokenIndex + NEXT_OP_DISTANCE
	}
	return currentExpr
}

func parseDateMathExpression(expr string) string {
	expr = strings.ReplaceAll(expr, "'", "")
	tokens := tokenizeDateMathExpr(expr)
	return buildDateMathExpression(tokens)
}

// DONE: tested in CH, it works for date format 'YYYY-MM-DDTHH:MM:SS.SSSZ'
// TODO:
//   - check if parseDateTime64BestEffort really works for our case (it should)
//   - implement "needed" date functions like now, now-1d etc.
func (cw *ClickhouseQueryTranslator) parseRange(queryMap QueryMap) SimpleQuery {
	// not checking for len == 1 because it's only option in proper query
	for field, v := range queryMap {
		field = cw.Table.ResolveField(field)
		stmts := make([]Statement, 0)
		if _, ok := v.(QueryMap); !ok {
			continue
		}
		isDatetimeInDefaultFormat := true // in 99% requests, format is "strict_date_optional_time", which we can parse with time.Parse(time.RFC3339Nano, ..)
		if format, ok := v.(QueryMap)["format"]; ok && format == "epoch_millis" {
			isDatetimeInDefaultFormat = false
		}

		for op, v := range v.(QueryMap) {
			fieldType := cw.Table.GetDateTimeType(field)
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
							vToPrint = parseDateMathExpression(vToPrint)
						}
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
							logger.Warn().Msgf("We use range with unknown literal %s, field %s", vToPrint, field)
						}
					}
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
			}
		}
		return newSimpleQueryWithFieldName(and(stmts), true, field)
	}
	return newSimpleQuery(NewSimpleStatement("empty range"), false)
}

// parseDateTimeString returns string used to parse DateTime in Clickhouse (depends on column type)
func (cw *ClickhouseQueryTranslator) parseDateTimeString(table *clickhouse.Table, field, dateTime string) string {
	typ := table.GetDateTimeType(field)
	switch typ {
	case clickhouse.DateTime64:
		return "parseDateTime64BestEffort('" + dateTime + "')"
	case clickhouse.DateTime:
		return "parseDateTimeBestEffort('" + dateTime + "')"
	case clickhouse.Invalid:
		logger.Error().Msgf("Invalid DateTime type for field: %s, parsed dateTime value: %s", field, dateTime)
	}
	return ""
}

// TODO: not supported:
// - The field has "index" : false and "doc_values" : false set in the mapping
// - The length of the field value exceeded an ignore_above setting in the mapping
// - The field value was malformed and ignore_malformed was defined in the mapping
func (cw *ClickhouseQueryTranslator) parseExists(queryMap QueryMap) SimpleQuery {
	// only parameter is 'field', must be string, so cast is safe
	sql := NewSimpleStatement("")
	for _, v := range queryMap {
		fieldName := v.(string)
		fieldName = cw.Table.ResolveField(fieldName)
		fieldNameQuoted := strconv.Quote(fieldName)

		switch cw.Table.GetFieldInfo(fieldName) {
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
		}
	}
	return newSimpleQuery(sql, true)
}

func (cw *ClickhouseQueryTranslator) extractFields(fields []interface{}) []string {
	result := make([]string, 0)
	for _, field := range fields {
		fieldStr := field.(string)
		if fieldStr == "*" {
			return cw.GetFieldsList() // careful: hardcoded for only "message" for now
		}
		fieldStr = cw.Table.ResolveField(fieldStr)
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
	if queryMapNested, ok = queryMap["aggs"].(QueryMap); !ok {
		return model.SearchQueryInfo{Typ: model.Normal}
	}
	if queryMapNested, ok = queryMapNested["suggestions"].(QueryMap); !ok {
		return model.SearchQueryInfo{Typ: model.Normal}
	}
	if queryMapNested, ok = queryMapNested["terms"].(QueryMap); !ok {
		return model.SearchQueryInfo{Typ: model.Normal}
	}
	if _, ok = queryMapNested["field"]; !ok {
		return model.SearchQueryInfo{Typ: model.Normal}
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
	fieldName := ""
	size := 0
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

	sizeAsAny, ok := firstNestingMap["size"]
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	} else {
		size = int(sizeAsAny.(float64))
	}
	fieldNameAsAny, ok := firstNestingMap["field"]
	if !ok {
		return model.NewSearchQueryInfoNone(), false
	}
	fieldName = strings.TrimSuffix(fieldNameAsAny.(string), ".keyword")
	fieldName = cw.Table.ResolveField(fieldName)

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
	size, okSize := queryMap["size"]
	_, okTrackTotalHits := queryMap["track_total_hits"]
	if okSize && okTrackTotalHits && len(queryMap) == 2 {
		// only ["size"] and ["track_total_hits"] are present
		return model.SearchQueryInfo{Typ: model.ListAllFields, FieldName: "*", I1: 0, I2: int(size.(float64))}, true
	}

	// 2) more general case:
	fields, ok := queryMap["fields"].([]interface{})
	if !ok || !okSize {
		return model.NewSearchQueryInfoNone(), false
	}
	if len(fields) > 1 {
		// so far everywhere I've seen, > 1 field ==> "*" is one of them
		return model.SearchQueryInfo{Typ: model.ListAllFields, FieldName: "*", I1: 0, I2: int(size.(float64))}, true
	} else if len(fields) == 0 {
		isCount, ok := queryMap["track_total_hits"].(bool)
		if ok && isCount {
			return model.SearchQueryInfo{Typ: model.CountAsync, FieldName: "", I1: 0, I2: 0}, true
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
			fieldName = queryMap["field"].(string)
		}

		resolvedField := cw.Table.ResolveField(fieldName)
		if resolvedField == "*" {
			return model.SearchQueryInfo{Typ: model.ListAllFields, FieldName: "*", I1: 0, I2: int(size.(float64))}, true
		}
		return model.SearchQueryInfo{Typ: model.ListByField, FieldName: resolvedField, I1: 0, I2: int(size.(float64))}, true
	}
}

func (cw *ClickhouseQueryTranslator) extractInterval(queryMap QueryMap) string {
	if fixedInterval, exists := queryMap["fixed_interval"]; exists {
		return fixedInterval.(string)
	}

	if calendarInterval, exists := queryMap["calendar_interval"]; exists {
		return calendarInterval.(string)
	}

	defaultInterval := "30s"
	logger.Warn().Msgf("histogram query, extractInterval: no interval found, returning default: %s", defaultInterval)
	return defaultInterval
}

// parseSortFields parses sort fields from the query
// We're skipping ELK internal fields, like "_doc", "_id", etc. (we only accept field starting with "_" if it exists in our table)
func (cw *ClickhouseQueryTranslator) parseSortFields(sortMaps []any) []string {
	sortFields := make([]string, 0)
	for _, sortMapAsAny := range sortMaps {
		sortMap, ok := sortMapAsAny.(QueryMap)
		if !ok {
			logger.Warn().Msgf("parseSortFields: unexpected type of value: %T", sortMapAsAny)
			continue
		}

		// sortMap has only 1 key, so we can just iterate over it
		for k, v := range sortMap {
			if strings.HasPrefix(k, "_") && cw.Table.GetFieldInfo(k) == clickhouse.NotExists {
				// we're skipping ELK internal fields, like "_doc", "_id", etc.
				continue
			}
			fieldName := cw.Table.ResolveField(k)
			if vAsMap, ok := v.(QueryMap); ok {
				if order, ok := vAsMap["order"]; ok {
					sortFields = append(sortFields, strconv.Quote(fieldName)+" "+order.(string))
				} else {
					sortFields = append(sortFields, strconv.Quote(fieldName))
				}
			} else {
				logger.Warn().Msgf("parseSortFields: unexpected type of value for key %s: %T", k, v)
			}
		}
	}
	return sortFields
}
