package queryparser

import (
	"encoding/json"
	"fmt"
	"github.com/k0kubun/pp"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/model"
	"strconv"
	"strings"
	"time"
)

type QueryMap = map[string]interface{}

type SimpleQuery struct {
	Sql       Statement
	CanParse  bool
	FieldName string
}

type Statement struct {
	Stmt       string
	isCompound bool // "a" -> not compound, "a AND b" -> compound. Used to not make unnecessary brackets (not always, but usually)
	FieldName  string
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

func NewSimpleStatementWithFieldName(stmt, fieldName string) Statement {
	return Statement{Stmt: stmt, isCompound: false, FieldName: fieldName}
}

func NewCompoundStatement(stmt string) Statement {
	return Statement{Stmt: stmt, isCompound: true}
}

func NewCompoundStatementWithFieldName(stmt, fieldName string) Statement {
	return Statement{Stmt: stmt, isCompound: true, FieldName: fieldName}
}

func (cw *ClickhouseQueryTranslator) ParseQuery(queryAsJson string) (SimpleQuery, model.SearchQueryType) {
	queryAsMap := make(QueryMap)
	err := json.Unmarshal([]byte(queryAsJson), &queryAsMap)
	if err != nil {
		return newSimpleQuery(NewSimpleStatement("invalid JSON (ParseQuery)"), false), model.Normal
	}
	queryInfo := cw.tryProcessMetadataSearch(queryAsMap)
	return cw.parseQueryMap(queryAsMap), queryInfo
}

func (cw *ClickhouseQueryTranslator) ParseQueryAsyncSearch(queryAsJson string) (SimpleQuery, model.QueryInfoAsyncSearch) {
	queryAsMap := make(QueryMap)
	err := json.Unmarshal([]byte(queryAsJson), &queryAsMap)
	if err != nil {
		return newSimpleQuery(NewSimpleStatement("invalid JSON (parseQueryAsyncSearch)"), false), model.NewQueryInfoAsyncSearchNone()
	}

	parsed := cw.parseQueryMap(queryAsMap["query"].(QueryMap))
	queryInfo := cw.tryProcessMetadataAsyncSearch(queryAsMap)
	/* leaving as comment, as that's how it'll work after next PR

	if queryInfo.Typ != model.None {
		// if we parsed it via old, non-general way, let's just use it for now, because it's known to be working
		return parsed, queryInfo
	}

	if aggs, ok := queryAsMap["aggs"].(QueryMap); ok {
		aggregations := make([]model.QueryWithAggregation, 0)
		currentAggr := aggrQueryBuilder{}
		cw.parseAggregation(&currentAggr, aggs, &aggregations)
		pp.Println(aggregations)
	}
	*/
	return parsed, queryInfo
}

// Metadata attributes are the ones that are on the same level as query tag
// They are moved into separate map for further processing if needed
func (cw *ClickhouseQueryTranslator) parseMetadata(queryMap QueryMap) QueryMap {
	queryMetadata := make(QueryMap, 5)
	for k, v := range queryMap {
		if k == "query" || k == "bool" || k == "query_string" { // probably change that, made so tests work, but let's see after more real use cases {
			continue
		}
		queryMetadata[k] = v
		delete(queryMap, k)
	}
	return queryMetadata
}

func (cw *ClickhouseQueryTranslator) parseQueryMap(queryMap QueryMap) SimpleQuery {
	if len(queryMap) != 1 {
		// TODO suppress metadata for now
		_ = cw.parseMetadata(queryMap)
	}
	parseMap := map[string]func(QueryMap) SimpleQuery{
		"match_all":           cw.parseMatchAll,
		"match":               cw.parseMatch,
		"multi_match":         cw.parseMultiMatch,
		"bool":                cw.parseBool,
		"term":                cw.parseTerm,
		"terms":               cw.parseTerms,
		"query":               cw.parseQueryMap,
		"prefix":              cw.parsePrefix,
		"nested":              cw.parseNested,
		"match_phrase":        cw.parseMatch,
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
			return newSimpleQuery(NewSimpleStatement(strconv.Quote(k)+"="+sprint(v)), true)
		}
	}
	return newSimpleQuery(NewSimpleStatement("invalid term len, != 1"), false)
}

// TODO remove optional parameters like boost
func (cw *ClickhouseQueryTranslator) parseTerms(queryMap QueryMap) SimpleQuery {
	if len(queryMap) == 1 {
		for k, v := range queryMap {
			vAsArray := v.([]interface{})
			orStmts := make([]Statement, len(vAsArray))
			for i, v := range vAsArray {
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

// TODO
// * support optional parameters
// - auto_generate_synonyms_phrase_query
// (Optional, Boolean) If true, match phrase queries are automatically created for multi-term synonyms. Defaults to true.
// - max_expansions
// (Optional, integer) Maximum number of terms to which the query will expand. Defaults to 50.
// - fuzzy_transpositions
// (Optional, Boolean) If true, edits for fuzzy matching include transpositions of two adjacent characters (ab → ba). Defaults to true.
// TOTHINK:
// - match_phrase also goes here. Maybe some different parsing is needed?
func (cw *ClickhouseQueryTranslator) parseMatch(queryMap QueryMap) SimpleQuery {
	if len(queryMap) == 1 {
		for k, v := range queryMap {
			// (k, v) = either e.g. ("message", "this is a test")
			//                  or  ("message", map["query": "this is a test", ...]). Here we only care about "query" until we find a case where we need more.
			vUnNested := v
			if vAsQueryMap, ok := v.(QueryMap); ok {
				vUnNested = vAsQueryMap["query"]
			}
			if vAsString, ok := vUnNested.(string); ok {
				split := strings.Split(vAsString, " ")
				qStrs := make([]Statement, len(split))
				for i, s := range split {
					qStrs[i] = NewSimpleStatement(strconv.Quote(k) + " iLIKE " + "'%" + s + "%'")
				}
				return newSimpleQuery(or(qStrs), true)
			}
			// so far we assume that only strings can be ORed here
			return newSimpleQuery(NewSimpleStatement(strconv.Quote(k)+" == "+sprint(vUnNested)), true)
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
		fields = cw.GetFieldsList(cw.TableName)
	}
	subQueries := strings.Split(queryMap["query"].(string), " ")
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
		for k, v := range queryMap {
			switch vCasted := v.(type) {
			case string:
				return newSimpleQuery(NewSimpleStatement(strconv.Quote(k)+" iLIKE '"+vCasted+"%'"), true)
			case QueryMap:
				return newSimpleQuery(NewSimpleStatement(strconv.Quote(k)+" iLIKE '"+vCasted["value"].(string)+"%'"), true)
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
	for k, v := range queryMap {
		return newSimpleQuery(NewSimpleStatement(strconv.Quote(k)+" iLIKE '"+strings.ReplaceAll(v.(QueryMap)["value"].(string),
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
			for _, qStr := range strings.Split(queryMap["query"].(string), " ") {
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
	if split[1][0] == '>' || split[1][0] == '<' {
		// to support fieldName>value, <value, etc. We see such request in Kibana
		return newSimpleQuery(NewSimpleStatement(split[0]+split[1]), true)
	}
	return newSimpleQuery(NewSimpleStatement(split[0]+" iLIKE '%"+split[1]+"%'"), true)
}

func (cw *ClickhouseQueryTranslator) parseNested(queryMap QueryMap) SimpleQuery {
	return cw.parseQueryMap(queryMap["query"].(QueryMap))
}

// DONE: tested in CH, it works for date format 'YYYY-MM-DDTHH:MM:SS.SSSZ'
// TODO:
//   - check if parseDateTime64BestEffort really works for our case (it should)
//   - implement "needed" date functions like now, now-1d etc.
func (cw *ClickhouseQueryTranslator) parseRange(queryMap QueryMap) SimpleQuery {
	// not checking for len == 1 because it's only option in proper query
	for field, v := range queryMap {
		stmts := make([]Statement, 0)
		for op, v := range v.(QueryMap) {
			vToPrint := sprint(v)
			if dateTime, ok := v.(string); ok {
				if _, err := time.Parse(time.RFC3339Nano, dateTime); err == nil {
					vToPrint = "parseDateTime64BestEffort('" + dateTime + "')"
				}
			}

			switch op {
			case "gte":
				stmts = append(stmts, NewSimpleStatement(strconv.Quote(field)+">="+vToPrint))
			case "lte":
				stmts = append(stmts, NewSimpleStatement(strconv.Quote(field)+"<="+vToPrint))
			case "gt":
				stmts = append(stmts, NewSimpleStatement(strconv.Quote(field)+">"+vToPrint))
			case "lt":
				stmts = append(stmts, NewSimpleStatement(strconv.Quote(field)+"<"+vToPrint))
			}
		}
		return newSimpleQueryWithFieldName(and(stmts), true, field)
	}
	return newSimpleQuery(NewSimpleStatement("empty range"), false)
}

// TODO: not supported:
// - The field has "index" : false and "doc_values" : false set in the mapping
// - The length of the field value exceeded an ignore_above setting in the mapping
// - The field value was malformed and ignore_malformed was defined in the mapping
func (cw *ClickhouseQueryTranslator) parseExists(queryMap QueryMap) SimpleQuery {
	// only parameter is 'field', must be string, so cast is safe
	sql := NewSimpleStatement("")
	for _, v := range queryMap {
		switch cw.ClickhouseLM.GetFieldInfo(cw.TableName, v.(string)) {
		case clickhouse.ExistsAndIsBaseType:
			sql = NewSimpleStatement(v.(string) + " IS NOT NULL")
		case clickhouse.ExistsAndIsArray:
			sql = NewSimpleStatement(v.(string) + ".size0 = 0")
		case clickhouse.NotExists:
			attrs := cw.ClickhouseLM.GetAttributesList(cw.TableName)
			stmts := make([]Statement, len(attrs))
			for i, a := range attrs {
				stmts[i] = NewCompoundStatement(fmt.Sprintf("has(%s,%s) AND %s[indexOf(%s,%s)] IS NOT NULL",
					strconv.Quote(a.KeysArrayName), strconv.Quote(v.(string)), strconv.Quote(a.ValuesArrayName),
					strconv.Quote(a.KeysArrayName), strconv.Quote(v.(string))))
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
			return cw.GetFieldsList(cw.TableName)
		}
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
		return NewCompoundStatementWithFieldName(sql, fieldName)
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

func (cw *ClickhouseQueryTranslator) tryProcessMetadataSearch(queryMap QueryMap) model.SearchQueryType {
	queryMap = cw.parseMetadata(queryMap) // TODO we can remove this if we need more speed. It's a bit unnecessary call.
	var ok bool
	if queryMap, ok = queryMap["aggs"].(QueryMap); !ok {
		return model.Normal
	}
	if queryMap, ok = queryMap["suggestions"].(QueryMap); !ok {
		return model.Normal
	}
	if queryMap, ok = queryMap["terms"].(QueryMap); !ok {
		return model.Normal
	}
	if _, ok = queryMap["field"]; !ok {
		return model.Normal
	}
	return model.Count
}

// Return value:
// - histogram: (Histogram, fixed interval, 0, 0)
// - aggsByField: (AggsByField, field name, nrOfGroupedBy, sampleSize)
// - listByField: (ListByField, field name, 0, LIMIT)
// - listAllFields: (ListAllFields, "*", 0, LIMIT) (LIMIT = how many rows we want to return)
func (cw *ClickhouseQueryTranslator) tryProcessMetadataAsyncSearch(queryMap QueryMap) model.QueryInfoAsyncSearch {
	metadata := cw.parseMetadata(queryMap) // TODO we can remove this if we need more speed. It's a bit unnecessary call, at least for now, when we're parsing brutally.
	// case 1: maybe it's a Histogram request:
	if queryInfo, ok := cw.isItHistogramRequest(metadata); ok {
		return queryInfo
	}

	// case 2: maybe it's a AggsByField request
	if queryInfo, ok := cw.isItAggsByFieldRequest(metadata); ok {
		return queryInfo
	}

	// case 3: maybe it's ListByField ListAllFields request
	if queryInfo, ok := cw.isItListRequest(metadata); ok {
		return queryInfo
	}

	// case 4: maybe it's EarliestLatestTimestamp request
	// If it's not, we (and isItEarliestLatestTimestampRequest) return QueryInfoNone
	queryInfo, _ := cw.isItEarliestLatestTimestampRequest(metadata)
	return queryInfo
}

// 'queryMap' - metadata part of the JSON query
// returns (info, true) if metadata shows it's histogram
// returns (model.NewQueryInfoAsyncSearchNone, false) if it's not histogram
func (cw *ClickhouseQueryTranslator) isItHistogramRequest(queryMap QueryMap) (model.QueryInfoAsyncSearch, bool) {
	queryMap, ok := queryMap["aggs"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	queryMapNestOnePossility, ok := queryMap["0"].(QueryMap)
	if ok {
		if queryMapNestOnePossility, ok = queryMapNestOnePossility["date_histogram"].(QueryMap); ok {
			if fixedInterval, exists := queryMapNestOnePossility["fixed_interval"]; exists {
				return model.QueryInfoAsyncSearch{Typ: model.Histogram, FieldName: fixedInterval.(string), I1: 0, I2: 0}, true
			}

			if calendarInterval, exists := queryMapNestOnePossility["calendar_interval"]; exists {
				return model.QueryInfoAsyncSearch{Typ: model.Histogram, FieldName: calendarInterval.(string), I1: 0, I2: 0}, true
			}
			return model.QueryInfoAsyncSearch{Typ: model.Histogram, FieldName: queryMapNestOnePossility["fixed_interval"].(string), I1: 0, I2: 0}, true
		}
	}

	queryMap, ok = queryMap["stats"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	queryMap, ok = queryMap["aggs"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	queryMap, ok = queryMap["series"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	queryMap, ok = queryMap["date_histogram"].(QueryMap)
	if ok {
		if fixedInterval, exists := queryMap["fixed_interval"]; exists {
			return model.QueryInfoAsyncSearch{Typ: model.Histogram, FieldName: fixedInterval.(string), I1: 0, I2: 0}, true
		}
		if calendarInterval, exists := queryMap["fixed_interval"]; exists {
			return model.QueryInfoAsyncSearch{Typ: model.Histogram, FieldName: calendarInterval.(string), I1: 0, I2: 0}, true
		}

		return model.QueryInfoAsyncSearch{Typ: model.Histogram, FieldName: queryMap["fixed_interval"].(string), I1: 0, I2: 0}, true

	}
	return model.NewQueryInfoAsyncSearchNone(), false
}

// 'queryMap' - metadata part of the JSON query
// returns (info, true) if metadata shows it's AggsByField request (used e.g. for facets in Kibana)
// returns (model.NewQueryInfoAsyncSearchNone, false) if it's not AggsByField request
func (cw *ClickhouseQueryTranslator) isItAggsByFieldRequest(queryMap QueryMap) (model.QueryInfoAsyncSearch, bool) {
	queryMap, ok := queryMap["aggs"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	fieldName := ""
	size := 0
	queryMap, ok = queryMap["sample"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	nestedOnePossibility, ok := queryMap["aggs"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	nestedOnePossibility, ok = nestedOnePossibility["top_values"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	nestedOnePossibility, ok = nestedOnePossibility["terms"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}

	size = int(nestedOnePossibility["size"].(float64))
	fieldName = strings.TrimSuffix(nestedOnePossibility["field"].(string), ".keyword")

	nestedSecondPossibility, ok := queryMap["sampler"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	shardSize, ok := nestedSecondPossibility["shard_size"].(float64)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	return model.QueryInfoAsyncSearch{Typ: model.AggsByField, FieldName: fieldName, I1: size, I2: int(shardSize)}, true
}

// 'queryMap' - metadata part of the JSON query
// returns (info, true) if metadata shows it's ListAllFields or ListByField request (used e.g. for listing all rows in Kibana)
// returns (model.NewQueryInfoAsyncSearchNone, false) if it's not ListAllFields/ListByField request
func (cw *ClickhouseQueryTranslator) isItListRequest(queryMap QueryMap) (model.QueryInfoAsyncSearch, bool) {
	fields, ok := queryMap["fields"].([]interface{})
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	size, ok := queryMap["size"].(float64)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	if len(fields) > 1 {
		// so far everywhere I've seen, > 1 field ==> "*" is one of them
		return model.QueryInfoAsyncSearch{Typ: model.ListAllFields, FieldName: "*", I1: 0, I2: int(size)}, true
	}
	if len(fields) == 0 {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	queryMap, ok = fields[0].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	// same as above
	field := queryMap["field"].(string)
	if field == "*" {
		return model.QueryInfoAsyncSearch{Typ: model.ListAllFields, FieldName: "*", I1: 0, I2: int(size)}, true
	}
	return model.QueryInfoAsyncSearch{Typ: model.ListByField, FieldName: field, I1: 0, I2: int(size)}, true
}

func (cw *ClickhouseQueryTranslator) isItEarliestLatestTimestampRequest(queryMap QueryMap) (model.QueryInfoAsyncSearch, bool) {
	queryMap, ok := queryMap["aggs"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}

	// min json
	minQueryMap, ok := queryMap["earliest_timestamp"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	minQueryMap, ok = minQueryMap["min"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	timestampFieldName1 := minQueryMap["field"].(string)

	// max json
	maxQueryMap, ok := queryMap["latest_timestamp"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	maxQueryMap, ok = maxQueryMap["max"].(QueryMap)
	if !ok {
		return model.NewQueryInfoAsyncSearchNone(), false
	}
	timestampFieldName2 := maxQueryMap["field"].(string)

	// probably unnecessary check, but just in case
	if timestampFieldName1 == timestampFieldName2 {
		return model.QueryInfoAsyncSearch{Typ: model.EarliestLatestTimestamp, FieldName: timestampFieldName1}, true
	}
	return model.NewQueryInfoAsyncSearchNone(), false
}
