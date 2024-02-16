package clickhouse

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/k0kubun/pp"
	"mitmproxy/quesma/util"
	"regexp"
	"sort"
	"strings"
)

func (lm *LogManager) DumpTableSchema(tableName string) (*Table, error) {
	columns := make(map[string]*Column)
	rows, err := lm.chDb.Query("SHOW COLUMNS FROM \"" + tableName + "\"")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name, typ string
		var s1, s2, s3, s4 sql.NullString
		err = rows.Scan(&name, &typ, &s1, &s2, &s3, &s4)
		// hack, when field support gets bigger, it'll only be worse.
		// probably need grammar or sth like that, soon
		typ = strings.Replace(typ, "DateTime64(3)", "DateTime64", -1)
		typ = strings.Replace(typ, "Object('json')", "Object", -1)
		if err != nil {
			return nil, err
		}
		parsedType, _ := parseTypeFromShowColumns(typ, name)
		columns[name] = &Column{Name: name, Type: parsedType}
	}
	return &Table{Name: tableName, Cols: columns, Config: NewOnlySchemaFieldsCHConfig()}, nil
}

func (lm *LogManager) DumpTableSchemas() (string, error) {
	if lm.chDb == nil {
		connection, err := sql.Open("clickhouse", lm.chUrl.String())
		if err != nil {
			return "", err
		}
		lm.chDb = connection
	}

	rows, err := lm.chDb.Query("SHOW TABLES")
	if err != nil {
		return "", err
	}

	result := make(TableMap)
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return "", err
		}
		schema, err := lm.DumpTableSchema(tableName)
		if err != nil {
			return "", err
		}
		result[tableName] = schema
	}
	return pp.Sprint(result), nil
}

// Code doesn't need to be pretty, 99.9% it's just for our purposes
// Parses type from SHOW COLUMNS FROM "table"
func parseTypeFromShowColumns(typ, name string) (Type, string) {
	// i1, i2, i3 indices of results of strings.Index
	// returns if i1 is closer to the beginning than i2 and i3
	isClosest := func(i1, i2, i3 int) bool {
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

	// s - type string
	var parseTypeRec func(s, colName string) (Type, string)
	parseTypeRec = func(s, colName string) (Type, string) {
		cols := make([]*Column, 0)
		finish := func() (Type, string) {
			if len(cols) == 1 {
				return cols[0].Type, colName
			} else {
				return MultiValueType{Name: "Tuple", Cols: cols}, colName
			}
		}
		for {
			if len(s) == 0 {
				return finish()
			}

			iSpace := strings.Index(s, " ")
			iComma := indexCommaSameIndentationLvl(s)
			iLeft := strings.Index(s, "(")
			iRight := strings.Index(s, ")")
			if iSpace == -1 && iComma == -1 && iLeft == -1 && iRight == -1 {
				cols = append(cols, &Column{Name: colName, Type: NewBaseType(s)})
				return finish()
			}

			name := ""
			if iSpace != -1 {
				name = s[:iSpace]
			} else if iLeft != -1 {
				name = s[:iLeft]
			}
			if isClosest(iLeft, iComma, iRight) { // '(' is closest
				if name == "Array" {
					baseType, _ := parseTypeRec(s[iLeft+1:], "")
					return CompoundType{Name: "Array", BaseType: baseType}, name
				} else {
					colType, _ := parseTypeRec(s[iLeft+1:], name)
					cols = append(cols, &Column{Name: "Tuple", Type: colType})
					if iComma != -1 {
						s = s[iComma+2:]
					} else {
						return finish() // check if needed to assign typ = typCasted
					}
				}
			} else {
				end := iComma
				if isClosest(iRight, iComma, -1) {
					end = iRight
				}
				cols = append(cols, &Column{Name: name, Type: NewBaseType(s[iSpace+1 : end])}) // TODO inspect type
			}
			if isClosest(iComma, iLeft, iRight) { // ',' closest (same ind lvl)
				// TODO
				// m[name] = nil
				s = s[iComma+2:]
			} else if isClosest(iRight, iLeft, iComma) { // ')' closest
				return finish() // TODO
			}
		}
	}
	return parseTypeRec(typ, name)
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
			s += fmt.Sprintf("%s\"%s\": ", util.Indent(i), key)
			nestedMap, ok := m[key].(SchemaMap)
			if ok {
				s += fmt.Sprintf("SchemaMap {\n%s%s},\n", helper(nestedMap, i+1), util.Indent(i))
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

// Replaces long full 'goType' type with short way to recreate it
//
//lint:ignore U1000 Ignore unused function, it's used manually to create 'schemas.go' file
func shortenDumpSchemasOutput(s string) string {
	findEndOfGoType := func(s string, i int) int {
		bracketsCnt := 0
		for i < len(s) {
			if s[i] == '{' {
				bracketsCnt++
			} else if s[i] == '}' {
				bracketsCnt--
			}
			if bracketsCnt == 0 {
				return i + 1
			}
			i++
		}
		return -1 // unreachable
	}
	r, _ := regexp.Compile(`Name:\s*"(.*)",\s*(goType:\s*&reflect\.rtype)`)
	x := r.FindAllSubmatchIndex([]byte(s), -1)
	result := ""
	i := 0
	for _, y := range x {
		result += s[i:y[4]] + `goType: NewBaseType("` + s[y[2]:y[3]] + `").goType`
		i = findEndOfGoType(s, y[5])
	}
	return strings.ReplaceAll(result+s[i:], "clickhouse.", "")
}
