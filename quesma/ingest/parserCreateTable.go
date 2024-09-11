// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"quesma/clickhouse"
	"quesma/logger"
	"unicode"
)

func omitWhitespace(q string, i int) int {
	for i < len(q) && unicode.IsSpace(rune(q[i])) {
		i++
	}
	if i >= len(q) {
		return -1
	}
	return i
}

// Omits whitespaces, then len(s) characters in 'q' need to match 's'
// Returns -1 if not matched, otherwise returns index after the match
func parseExact(q string, i int, s string) int {
	i = omitWhitespace(q, i)
	if i+len(s) > len(q) {
		return -1
	}
	if q[i:i+len(s)] == s {
		return i + len(s)
	}
	return -1
}

// bool -> if found
func parseMaybeAndForget(q string, i int, s string) (int, bool) {
	i = omitWhitespace(q, i)
	if i+len(s) > len(q) {
		return i, false
	}
	if q[i:i+len(s)] == s {
		return i + len(s), true
	}
	return i, false
}

func parseMaybeAndForgetMultiple(q string, i int, ss []string) (int, bool) {
	for _, s := range ss {
		i2, ok := parseMaybeAndForget(q, i, s)
		if ok {
			return i2, true
		}
	}
	return i, false
}

func isGoodIdentChar(r rune) bool {
	return !unicode.IsSpace(r) && r != ')' && r != '"' && r != '`' && r != ',' && r != '('
}

// TODO idents starting with digit accepted, shouldn't probably be
// parse identificator in q[i:]
func parseIdent(q string, i int) (int, string) {
	i = omitWhitespace(q, i)
	if i >= len(q) {
		return -1, ""
	}
	if !isGoodIdentChar(rune(q[i])) {
		return -1, ""
	}
	j := i + 1
	for j < len(q) && isGoodIdentChar(rune(q[j])) {
		j++
	}
	return j, q[i:j]
}

func parseNullable(q string, i int) (int, clickhouse.Type) {
	i = omitWhitespace(q, i)
	if q[i] == 'N' {
		i, ok := parseMaybeAndForget(q, i, "Nullable")
		if ok {
			i = parseExact(q, i, "(")
			if i == -1 {
				return -1, nil
			}
			i, ident := parseType(q, i)
			if i == -1 {
				return -1, nil
			}
			i = parseExact(q, i, ")")
			if i == -1 {
				return -1, nil
			}
			typeAsBaseType, ok := ident.(clickhouse.BaseType)
			if ok {
				typeAsBaseType.Nullable = true
				return i, typeAsBaseType
			} else {
				logger.Warn().Msgf("Only BaseTypes can be Nullable! Here type is not BaseType, but %T", ident)
			}
			return i, ident
		}
	}
	return parseType(q, i)
}

// Returns -1 if not matched, otherwise returns (index after the match, ident)
func parseIdentWithBrackets(q string, i int) (int, string) {
	i = omitWhitespace(q, i)
	if i >= len(q) {
		return -1, ""
	}
	b, e := i, i
	bracketsCnt := 0
	for i < len(q) {
		if q[i] == '(' {
			e = i
			bracketsCnt++
		} else if q[i] == ')' {
			bracketsCnt--
		}
		if bracketsCnt == 0 && (q[i] == ' ' || q[i] == ',' || q[i] == ')') {
			return i + 1, q[b:e]
		}
		i++
	}
	return -1, ""
}

func parseColumn(q string, i int) (int, clickhouse.Column) {
	col := clickhouse.Column{}
	i = omitWhitespace(q, i)
	// name
	quote := `"`
	i2 := parseExact(q, i, quote)
	if i2 == -1 {
		quote = "`"
		i2 = parseExact(q, i, quote)
		if i2 == -1 {
			return -1, col
		}
	}
	i, col.Name = parseIdent(q, i2)
	if i == -1 {
		return -1, col
	}
	i = parseExact(q, i, quote)
	// type
	if i == -1 {
		return -1, col
	}
	i, col.Type = parseNullable(q, i)
	if i == -1 {
		return -1, col
	}

	// NULL | NOT NULL
	i = omitWhitespace(q, i)
	i, _ = parseMaybeAndForgetMultiple(q, i, []string{"NULL", "NOT NULL"})

	// DEFAULT | MATERIALIZED | EPHEMERAL | ALIAS expr
	i = omitWhitespace(q, i)
	i, ok := parseMaybeAndForgetMultiple(q, i, []string{"DEFAULT", "MATERIALIZED", "EPHEMERAL", "ALIAS"})
	if ok {
		i = omitWhitespace(q, i)
		i = parseExpr(q, i)
		if i == -1 {
			return -1, col
		}
		i = omitWhitespace(q, i)
	}

	// CODEC
	if i+5 < len(q) && q[i:i+5] == "CODEC" {
		i, col.Codec = parseCodec(q, i)
		i = omitWhitespace(q, i)
	}

	// TTL
	if i+3 < len(q) && q[i:i+3] == "TTL" {
		i = omitWhitespace(q, i+3)
		i = parseExpr(q, i)
		if i == -1 {
			return -1, col
		}
		i = omitWhitespace(q, i)
	}

	// COMMENT
	if i+7 < len(q) && q[i:i+7] == "COMMENT" {
		return -1, col // TODO unsupported: parse comment, last thing to parse, let's do it later
	}

	if i == -1 || i >= len(q) || (q[i] != ',' && q[i] != ')') {
		return -1, col
	}
	return i, col
}

func parseType(q string, i int) (int, clickhouse.Type) {
	i2, name := parseIdent(q, i)
	if i == -1 {
		return -1, nil
	}
	switch name {
	case "Array":
		i, baseType := parseCompoundType(q, i2)
		if i == -1 {
			return -1, nil
		}
		return i, clickhouse.CompoundType{Name: name, BaseType: baseType}
	case "Tuple", "Nested":
		i, types := parseMultiValueType(q, i2)
		if i == -1 {
			return -1, nil
		}
		return i, clickhouse.MultiValueType{Name: name, Cols: types}
	}
	if parseExact(q, i2, "(") != -1 {
		i, name = parseIdentWithBrackets(q, i)
		if i == -1 {
			return -1, nil
		}
		return i, clickhouse.NewBaseType(name)
	} else {
		return i2, clickhouse.NewBaseType(name)
	}
}

func parseCompoundType(q string, i int) (int, clickhouse.Type) {
	i = parseExact(q, i, "(")
	if i == -1 {
		return -1, nil
	}
	i, typ := parseNullable(q, i)
	if i == -1 {
		return -1, nil
	}
	i = parseExact(q, i, ")")
	if i == -1 {
		return -1, nil
	}
	return i, typ
}

// parseMultiValueType returns -1 if failed, otherwise (index after the match, []*Column)
// TO THINK: subcolumns shouldn't have codecs? Maybe fix it somehow
// TODO maybe merge with 'parseColumn'? Can wait, for now it works as it is.
func parseMultiValueType(q string, i int) (int, []*clickhouse.Column) {
	i = parseExact(q, i, "(")
	if i == -1 {
		return -1, nil
	}
	var subColumns []*clickhouse.Column
	for {
		i = omitWhitespace(q, i)
		quote := " "
		if q[i] == '"' || q[i] == '`' {
			quote = string(q[i])
			i++
		}
		j, name := parseIdent(q, i)
		if j == -1 || (quote != " " && string(q[j]) != quote) {
			return -1, nil
		}
		if quote != " " {
			j++
		}
		j = omitWhitespace(q, j)
		j, typ := parseNullable(q, j)
		if j == -1 {
			return -1, nil
		}
		subColumns = append(subColumns, &clickhouse.Column{Name: name, Type: typ})
		j = omitWhitespace(q, j)
		if q[j] == ')' {
			return j + 1, subColumns
		}
		if q[j] != ',' {
			return -1, nil
		}
		i = omitWhitespace(q, j+1)
	}
}

func parseCodec(q string, i int) (int, clickhouse.Codec) {
	b := i
	i = parseExact(q, i, "CODEC")
	if i == -1 {
		return -1, clickhouse.Codec{}
	}
	i = omitWhitespace(q, i)
	i = parseExact(q, i, "(")
	bracketsCnt := 1
	for i < len(q) && bracketsCnt > 0 {
		if q[i] == '(' {
			bracketsCnt++
		} else if q[i] == ')' {
			bracketsCnt--
		}
		i++
	}
	if i >= len(q) {
		return -1, clickhouse.Codec{}
	}
	return i, clickhouse.Codec{Name: q[b:i]}
}

// Kind of hackish, but should work 100% of the time, unless CODEC/TTL/COMMENT
// can be used in expressions (I'd assume they can't)
func parseExpr(q string, i int) int {
	bracketsCnt := 0
	for i < len(q) {
		if q[i] == '(' {
			bracketsCnt++
		} else if q[i] == ')' {
			bracketsCnt--
		}
		if bracketsCnt < 0 {
			return i
		}
		if bracketsCnt == 0 {
			if q[i] == ',' {
				return i
			}
			_, ok := parseMaybeAndForgetMultiple(q, i, []string{"CODEC", "TTL", "COMMENT"})
			if ok {
				return i
			}
			if q[i] == ')' {
				i2 := omitWhitespace(q, i+1)
				if parseExact(q, i2, "ENGINE") != -1 {
					return i
				}
			}
		}
		i = omitWhitespace(q, i+1)
	}
	return -1
}
