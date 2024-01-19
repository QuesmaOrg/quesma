package quesma

import (
	"encoding/json"
	"fmt"
	"github.com/k0kubun/pp"
	"strings"
)

type Query struct {
	sql      string
	canParse bool
}
type JsonMap = map[string]interface{}

const tableName = "table"

func NewQuery(sql string, canParse bool) Query {
	return Query{sql, canParse}
}

// 'q' - string of a JSON query
func parseQuery(q string) Query {
	m := make(JsonMap)
	err := json.Unmarshal([]byte(q), &m)
	if err != nil {
		return NewQuery("Invalid JSON", false)
	}
	parsed := parseJsonMap(m)
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

func parseJsonMap(m JsonMap) Query {
	if len(m) != 1 {
		return NewQuery("parseJsonMap len(m) should be 1", false)
	}
	parseMap := map[string]func(JsonMap) Query{
		"match_all": parseMatchAll,
		"match":     parseMatch,
		"bool":      parseBool,
		"term":      parseTerm,
		"query":     parseJsonMap,
	}
	for k, v := range m {
		fmt.Println("k:", k, "v:", v)
		if f, ok := parseMap[k]; ok {
			return f(v.(JsonMap))
		}
	}
	return NewQuery("Can't parse query: "+pp.Sprint(m), false)
}

func parseJsonMapArray(m []interface{}) []string {
	results := make([]string, len(m))
	for i, v := range m {
		results[i] = parseJsonMap(v.(JsonMap)).sql
	}
	return results
}

func parseBool(m JsonMap) Query {
	/*  for andPhrase in ['must', 'filter']:
	  if andPhrase in bool_json:
	    for el in iterateListOrDictionary(bool_json[andPhrase]):
	      mustOrFiltCount += 1
	      results.append(_parse_query(el))
	minimum_should_match = 1
	if 'minimum_should_match' in bool_json:
	  minimum_should_match = bool_json['minimum_should_match']
	  if minimum_should_match != 0 or minimum_should_match != 1:
	    comments.append('Skipping {minimum_should_match} minimum_should_match, assuming 1')
	    minimum_should_match = 1
	else:
	  if mustOrFiltCount > 1:
	    minimum_should_match = 0

	if minimum_should_match == 1:
	  resultsOr = []
	  if 'should' in bool_json:
	    for el in iterateListOrDictionary(bool_json['should']):
	      resultsOr.append(_parse_query(el))
	  if len(resultsOr) > 0:
	    results.append(createResultOr(resultsOr))

	# Must not
	if 'must_not' in bool_json:
	  resultsNot = []
	  for el in iterateListOrDictionary(bool_json['must_not']):
	    resultsNot.append(_parse_query(el))
	  if len(resultsNot) > 0:
	    results.append(createNot(createResultOr(resultsNot)))
	# print("  _parse_bool, bool_json: ", bool_json, " results: ", results[0])
	return createResultAnd(results)
	*/
	switch mt := m["filter"].(type) {
	case []interface{}:
		return NewQuery(strings.Join(parseJsonMapArray(mt), " AND "), true)
	case JsonMap:
		return parseJsonMap(mt)
	default:
		fmt.Print("wtf")
	}
	return NewQuery("Invalid bool "+pp.Sprint(m["filter"]), false)
}

func parseTerm(m JsonMap) Query {
	if len(m) == 1 {
		for k, v := range m {
			return NewQuery(k+"="+fmt.Sprintf("%v", v), true)
		}
	}
	return NewQuery("Invalid term", false)
}

func parseMatchAll(m JsonMap) Query {
	return NewQuery("", true)
}

func parseMatch(m JsonMap) Query {
	if len(m) == 1 {
		for k, v := range m {
			return NewQuery(k+"="+v.(string), true)
		}
	}
	return NewQuery("Invalid match", false)
}
