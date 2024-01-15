package clickhouse

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// For later we'll probably need a better parser, probably a grammar
func (lm *LogManager) DumpTableSchema(table string) (interface{}, error) {
	// i1, i2, i3 indices of results of strings.Index
	// returns if i1 is closer to the beginning than i2 and i3
	closest := func(i1, i2, i3 int) bool {
		return i1 != -1 && (i1 < i2 || i2 == -1) && (i1 < i3 || i3 == -1)
	}
	// '(' -> indentationLvl++
	// ')' -> indentationLvl--
	// We want a comma when indentationLvl == 0, so we have a new field
	indexCommaSameIndentationLvl := func(s string) int {
		lvl := 0
		for i, c := range s {
			if c == ',' && lvl == 0 {
				return i
			} else if c == '(' {
				lvl++
			} else if c == ')' {
				lvl--
			}
			if lvl < 0 {
				return -1
			}
		}
		return -1
	}
	var parseType func(string) SchemaMap
	parseType = func(s string) SchemaMap {
		m := make(SchemaMap)

		for {
			if len(s) == 0 {
				return m
			}

			iSpace := strings.Index(s, " ")
			iComma := indexCommaSameIndentationLvl(s)
			iLeft := strings.Index(s, "(")
			iRight := strings.Index(s, ")")
			name := s[:iSpace]
			if closest(iLeft, iComma, iRight) { // '(' is closest
				m[name] = parseType(s[iLeft+1:])
				if iComma != -1 {
					s = s[iComma+2:]
				} else {
					return m
				}
			} else if closest(iComma, iLeft, iRight) { // ',' closest (same ind lvl)
				m[name] = nil
				s = s[iComma+2:]
			} else { // ')' closest
				m[name] = nil
				return m
			}
		}
	}

	columns := make(SchemaMap)
	rows, err := lm.db.Query("SHOW COLUMNS FROM \"" + table + "\"")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name, Type string
		var s1, s2, s3, s4 sql.NullString
		err = rows.Scan(&name, &Type, &s1, &s2, &s3, &s4)
		// hack, when field support gets bigger, it'll only be worse.
		// probably need grammar or sth like that, soon
		Type = strings.Replace(Type, "DateTime64(3)", "DateTime64", -1)
		Type = strings.Replace(Type, "Object('json')", "Object", -1)
		if err != nil {
			return nil, err
		}
		i := strings.Index(Type, "(")
		if i == -1 {
			columns[name] = nil
		} else {
			columns[name] = parseType(Type[i+1:])
		}
	}
	return columns, nil
}

func (lm *LogManager) DumpTableSchemas() (SchemaMap, error) {
	if lm.db == nil {
		connection, err := sql.Open("clickhouse", url)
		if err != nil {
			return nil, err
		}
		lm.db = connection
	}

	rows, err := lm.db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}

	result := make(SchemaMap)
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		schema, err := lm.DumpTableSchema(table)
		if err != nil {
			return nil, err
		}
		result[table] = schema
	}
	return result, nil
}

func PrettyPrint(m SchemaMap) string {
	var helper func(SchemaMap, int) string

	helper = func(m SchemaMap, i int) string {
		s := ""
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, key := range keys {
			s += fmt.Sprintf("%s\"%s\": ", indent(i), key)
			nestedMap, ok := m[key].(SchemaMap)
			if ok {
				s += fmt.Sprintf("SchemaMap {\n%s%s},\n", helper(nestedMap, i+1), indent(i))
			} else {
				s += "nil,\n"
			}
		}
		return s
	}
	name := "n"
	return name + " := map[string]SchemaMap {\n" + helper(m, 1) + "}"
}

func PrettyJson(jsonStr string) string {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, []byte(jsonStr), "", "    "); err != nil {
		return fmt.Sprintf("PrettyJson err: %v\n", err)
	}
	return prettyJSON.String()
}
