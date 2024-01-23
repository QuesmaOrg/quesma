package quesma

import (
	"encoding/json"
	"fmt"
	"mitmproxy/quesma/clickhouse"
	"strings"
	"time"

	"github.com/k0kubun/pp"
)

type Query struct {
	sql      string
	canParse bool
}
type JsonMap = map[string]interface{}

const tableName = `"logs-generic-default"`

func NewQuery(sql string, canParse bool) Query {
	return Query{sql, canParse}
}

// 'q' - string of a JSON query
func (cw *ClickhouseQueryTranslator) parseQuery(q string) Query {
	m := make(JsonMap)
	err := json.Unmarshal([]byte(q), &m)
	if err != nil {
		return NewQuery("Invalid JSON (parseQuery)", false)
	}
	parsed := cw.parseJsonMap(m)
	if !parsed.canParse {
		return parsed
	} else {
		where := " WHERE "
		if len(parsed.sql) == 0 {
			where = ""
		}
		return NewQuery("SELECT * FROM "+tableName+where+parsed.sql, true)
	}
}

// Metadata attributes are the ones that are on the same level as query tag
// They are moved into separate map for further processing if needed
func (cw *ClickhouseQueryTranslator) parseMetadata(m JsonMap) map[string]interface{} {
	queryMetadata := make(map[string]interface{}, 5)
	for k, v := range queryMetadata {
		if k == "query" {
			continue
		}
		queryMetadata[k] = v
		delete(m, k)
	}
	return queryMetadata
}

func (cw *ClickhouseQueryTranslator) parseJsonMap(m JsonMap) Query {
	if len(m) != 1 {
		// TODO suppress metadata for now
		_ = cw.parseMetadata(m)
	}
	parseMap := map[string]func(JsonMap) Query{
		"match_all":           cw.parseMatchAll,
		"match":               cw.parseMatch,
		"multi_match":         cw.parseMultiMatch,
		"bool":                cw.parseBool,
		"term":                cw.parseTerm,
		"terms":               cw.parseTerms,
		"query":               cw.parseJsonMap,
		"prefix":              cw.parsePrefix,
		"nested":              cw.parseNested,
		"match_phrase":        cw.parseMatch,
		"range":               cw.parseRange,
		"exists":              cw.parseExists,
		"wildcard":            cw.parseWildcard,
		"query_string":        cw.parseQueryString,
		"simple_query_string": cw.parseQueryString,
	}
	for k, v := range m {
		if f, ok := parseMap[k]; ok {
			return f(v.(JsonMap))
		}
	}
	return NewQuery("Can't parse query: "+pp.Sprint(m), false)
}

// Parses each query separately, returns list of translated SQLs
func (cw *ClickhouseQueryTranslator) parseJsonMapArray(m []interface{}) []string {
	results := make([]string, len(m))
	for i, v := range m {
		results[i] = cw.parseJsonMap(v.(JsonMap)).sql
	}
	return results
}

func (cw *ClickhouseQueryTranslator) iterateListOrDict(m interface{}) []string {
	switch mt := m.(type) {
	case []interface{}:
		return cw.parseJsonMapArray(mt)
	case JsonMap:
		return []string{cw.parseJsonMap(mt).sql}
	default:
		return []string{"Invalid iteration"}
	}
}

// TODO: minimum_should_match parameter. Now only ints supported and >1 changed into 1
func (cw *ClickhouseQueryTranslator) parseBool(m JsonMap) Query {
	andStmts := []string{}
	for _, andPhrase := range []string{"must", "filter"} {
		if q, ok := m[andPhrase]; ok {
			andStmts = append(andStmts, cw.iterateListOrDict(q)...)
		}
	}
	sql := and(andStmts)

	minimumShouldMatch := 0
	if v, ok := m["minimum_should_match"]; ok {
		minimumShouldMatch = int(v.(float64))
	}
	if len(andStmts) == 0 || minimumShouldMatch > 1 {
		minimumShouldMatch = 1
	}
	if q, ok := m["should"]; ok && minimumShouldMatch == 1 {
		orSql := or(cw.iterateListOrDict(q))
		if len(andStmts) == 0 {
			sql = orSql
		} else if len(orSql) > 0 {
			sql = and([]string{sql, orSql})
		}
	}

	if q, ok := m["must_not"]; ok {
		sqlNots := cw.iterateListOrDict(q)
		if len(sqlNots) > 0 {
			sql = and([]string{sql, "NOT " + or(sqlNots)})
		}
	}
	return NewQuery(sql, true)
}

func (cw *ClickhouseQueryTranslator) parseTerm(m JsonMap) Query {
	if len(m) == 1 {
		for k, v := range m {
			return NewQuery(k+"="+sprint(v), true)
		}
	}
	return NewQuery("Invalid term len, != 1", false)
}

// TODO remove optional parameters like boost
func (cw *ClickhouseQueryTranslator) parseTerms(m JsonMap) Query {
	if len(m) == 1 {
		for k, v := range m {
			vc := v.([]interface{})
			orStmts := make([]string, len(vc))
			for i, v := range vc {
				orStmts[i] = k + "=" + sprint(v)
			}
			return NewQuery(or(orStmts), true)
		}
	}
	return NewQuery("Invalid terms len, != 1", false)
}

func (cw *ClickhouseQueryTranslator) parseMatchAll(m JsonMap) Query {
	return NewQuery("", true)
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
// - casting to string. 'Match' on e.g. ints doesn't make sense, does it?
// - match_phrase also goes here. Maybe some different parsing is needed?
func (cw *ClickhouseQueryTranslator) parseMatch(m JsonMap) Query {
	if len(m) == 1 {
		for k, v := range m {
			split := strings.Split(v.(string), " ")
			qStrs := make([]string, len(split))
			for i, s := range split {
				qStrs[i] = k + " iLIKE " + "'%" + s + "%'"
			}
			return NewQuery(or(qStrs), true)
		}
	}
	return NewQuery("Unsupported match len != 1", false)
}

func (cw *ClickhouseQueryTranslator) parseMultiMatch(m JsonMap) Query {
	var fields []string
	fieldsAsInterface, ok := m["fields"]
	if ok {
		fields = cw.extractFields(fieldsAsInterface.([]interface{}))
	} else {
		fields = cw.getFieldsList(tableName)
	}
	subQs := strings.Split(m["query"].(string), " ")
	sqls := make([]string, len(fields)*len(subQs))
	i := 0
	for _, field := range fields {
		for _, subQ := range subQs {
			sqls[i] = field + " iLIKE '%" + subQ + "%'"
			i++
		}
	}
	return NewQuery(or(sqls), true)
}

// prefix works only on strings
func (cw *ClickhouseQueryTranslator) parsePrefix(m JsonMap) Query {
	if len(m) == 1 {
		for k, v := range m {
			switch vc := v.(type) {
			case string:
				return NewQuery(k+" iLIKE '"+vc+"%'", true)
			case JsonMap:
				return NewQuery(k+" iLIKE '"+vc["value"].(string)+"%'", true)
			}
		}
	}
	return NewQuery("Invalid prefix len != 1", false)
}

// Not supporting 'case_insensitive' (optional)
// Also not supporting wildcard (Required, string) (??) In both our example, and their in docs,
// it's not provided.
func (cw *ClickhouseQueryTranslator) parseWildcard(m JsonMap) Query {
	// not checking for len == 1 because it's only option in proper query
	for k, v := range m {
		return NewQuery(k+" iLIKE '"+strings.ReplaceAll(v.(JsonMap)["value"].(string),
			"*", "%")+"'", true)
	}
	return NewQuery("Empty wildcard", false)
}

// This one is REALLY complicated (https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html)
// Supporting 'fields' and 'query' (also, * in 'fields' doesn't support other types than string...)
// + only '*' in query, no '?' or other regex
func (cw *ClickhouseQueryTranslator) parseQueryString(m JsonMap) Query {
	orStmts := make([]string, 0)
	fields := cw.extractFields(m["fields"].([]interface{}))
	for _, field := range fields {
		for _, qStr := range strings.Split(m["query"].(string), " ") {
			orStmts = append(orStmts, field+" iLIKE '%"+strings.ReplaceAll(qStr, "*", "%")+"%'")
		}
	}
	return NewQuery(or(orStmts), true)
}

func (cw *ClickhouseQueryTranslator) parseNested(m JsonMap) Query {
	return cw.parseJsonMap(m["query"].(JsonMap))
}

// DONE: tested in CH, it works for date format 'YYYY-MM-DDTHH:MM:SS.SSSZ'
// TODO:
//   - check if parseDateTime64BestEffort really works for our case (it should)
//   - implement "needed" date functions like now, now-1d etc.
func (cw *ClickhouseQueryTranslator) parseRange(m JsonMap) Query {
	// not checking for len == 1 because it's only option in proper query
	for field, v := range m {
		stmts := make([]string, 0)
		for op, v := range v.(JsonMap) {
			vToPrint := sprint(v)
			s, ok := v.(string)
			if ok {
				_, err := time.Parse(time.RFC3339Nano, s)
				if err == nil {
					vToPrint = "parseDateTime64BestEffort('" + s + "')"
				}
			}

			switch op {
			case "gte":
				stmts = append(stmts, field+">="+vToPrint)
			case "lte":
				stmts = append(stmts, field+"<="+vToPrint)
			case "gt":
				stmts = append(stmts, field+">"+vToPrint)
			case "lt":
				stmts = append(stmts, field+"<"+vToPrint)
			}
		}
		return NewQuery(and(stmts), true)
	}
	return NewQuery("Empty range", false)
}

// TODO: not supported
// - The field has "index" : false and "doc_values" : false set in the mapping
// - The length of the field value exceeded an ignore_above setting in the mapping
// - The field value was malformed and ignore_malformed was defined in the mapping
func (cw *ClickhouseQueryTranslator) parseExists(m JsonMap) Query {
	// only parameter is 'field', must be string, so cast is safe
	sql := ""
	for _, v := range m {
		switch cw.getFieldInfo(tableName, v.(string)) {
		case clickhouse.ExistsAndIsBaseType:
			sql = v.(string) + " IS NOT NULL"
		case clickhouse.ExistsAndIsArray:
			sql = v.(string) + ".size0 = 0"
		case clickhouse.NotExists:
			attrs := cw.getAttributesList(tableName)
			stmts := make([]string, len(attrs))
			for i, a := range attrs {
				stmts[i] = fmt.Sprintf("has(%s,%s) AND %s[indexOf(%s,%s)] IS NOT NULL",
					a.KeysArrayName, v.(string), a.ValuesArrayName, a.KeysArrayName, v.(string))
			}
			sql = or(stmts)
		}
	}
	return NewQuery(sql, true)
}

func (cw *ClickhouseQueryTranslator) extractFields(fields []interface{}) []string {
	result := make([]string, 0)
	for _, field := range fields {
		fieldStr := field.(string)
		if fieldStr == "*" {
			return cw.getFieldsList(tableName)
		}
		result = append(result, fieldStr)
	}
	return result
}

func combineStatements(stmts []string, sep string) string {
	s := strings.Join(stmts, " "+sep+" ")
	if len(stmts) > 1 {
		return "(" + s + ")"
	}
	return s
}
func and(andStmts []string) string {
	return combineStatements(andStmts, "AND")
}

func or(orStmts []string) string {
	return combineStatements(orStmts, "OR")
}

func sprint(i interface{}) string {
	switch i.(type) {
	case string:
		return fmt.Sprintf("'%v'", i)
	case map[string]interface{}:
		iface := i
		mapType := iface.(map[string]interface{})
		value := mapType["value"]
		return sprint(value)
	default:
		return fmt.Sprintf("%v", i)
	}
}
