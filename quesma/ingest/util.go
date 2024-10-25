// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"quesma/clickhouse"
	"strings"
)

type TableColumNameFormatter interface {
	Format(namespace, columnName string) string
}

type columNameFormatter struct {
	separator string
}

func (t *columNameFormatter) Format(namespace, columnName string) string {
	if namespace == "" {
		return columnName
	}
	return fmt.Sprintf("%s%s%s", namespace, t.separator, columnName)
}

func DefaultColumnNameFormatter() TableColumNameFormatter {
	return &columNameFormatter{separator: "_"}
}

// Code doesn't need to be pretty, 99.9% it's just for our purposes
// Parses type from SHOW COLUMNS FROM "table"
func parseTypeFromShowColumns(typ, name string) (clickhouse.Type, string) {
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
	var parseTypeRec func(s, colName string) (clickhouse.Type, string)
	parseTypeRec = func(s, colName string) (clickhouse.Type, string) {
		cols := make([]*clickhouse.Column, 0)
		finish := func() (clickhouse.Type, string) {
			if len(cols) == 1 {
				return cols[0].Type, colName
			} else {
				return clickhouse.MultiValueType{Name: "Tuple", Cols: cols}, colName
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
				cols = append(cols, &clickhouse.Column{Name: colName, Type: clickhouse.NewBaseType(s)})
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
					return clickhouse.CompoundType{Name: "Array", BaseType: baseType}, name
				} else {
					colType, _ := parseTypeRec(s[iLeft+1:], name)
					cols = append(cols, &clickhouse.Column{Name: "Tuple", Type: colType})
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
				cols = append(cols, &clickhouse.Column{Name: name, Type: clickhouse.NewBaseType(s[iSpace+1 : end])}) // TODO inspect type
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

func PrettyJson(jsonStr string) string {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, []byte(jsonStr), "", "    "); err != nil {
		return fmt.Sprintf("PrettyJson err: %v\n", err)
	}
	return prettyJSON.String()
}
