// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"sort"
	"strings"
)

// Highlighter is a struct that holds information about highlighted fields.
//
// An instance of highlighter is created for each query and is a result of query parsing process,
// so that Fields, PreTags, PostTags are set.
// Once Query is parsed, highlighter visitor is used to traverse the AST and extract tokens
// which should be highlighted.
//
// You can read more in:
//   - https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html
//   - https://medium.com/@andre.luiz1987/using-highlighting-elasticsearch-9ccd698f08

type Highlighter struct {
	// Tokens is a map of field/column name to a set of tokens which should be highlighted.
	Tokens map[string]Tokens

	PreTags  []string
	PostTags []string
}

// Tokens represents a set of tokens which should be highlighted.
type Tokens map[string]struct{}

// GetSortedTokens returns a length-wise sorted list of tokens,
// so that highlight results are deterministic and larger chunks are highlighted first.
func (h *Highlighter) GetSortedTokens(columnName string) []string {
	var tokensList []string
	for token := range h.Tokens[columnName] {
		tokensList = append(tokensList, token)
	}
	sort.Slice(tokensList, func(i, j int) bool {
		return len(tokensList[i]) > len(tokensList[j])
	})
	return tokensList
}

func (h *Highlighter) ShouldHighlight(columnName string) bool {
	_, ok := h.Tokens[columnName]
	return ok
}

// SetTokensToHighlight takes a Select query and extracts tokens that should be highlighted.
func (h *Highlighter) SetTokensToHighlight(selectCmd SelectCommand) {

	h.Tokens = make(map[string]Tokens)

	visitor := NewBaseVisitor()

	visitor.OverrideVisitInfix = func(b *BaseExprVisitor, e InfixExpr) interface{} {
		switch e.Op {
		case "iLIKE", "LIKE", "IN", "=", MatchOperator:
			lhs, isColumnRef := e.Left.(ColumnRef)
			rhs, isLiteral := e.Right.(LiteralExpr)
			if isLiteral && isColumnRef { // we only highlight in this case
				switch literalAsString := rhs.Value.(type) {
				case string:
					literalAsString = strings.TrimPrefix(literalAsString, "'")
					literalAsString = strings.TrimPrefix(literalAsString, "%")
					literalAsString = strings.TrimSuffix(literalAsString, "'")
					literalAsString = strings.TrimSuffix(literalAsString, "%")
					if h.Tokens[lhs.ColumnName] == nil {
						h.Tokens[lhs.ColumnName] = make(Tokens)
					}
					h.Tokens[lhs.ColumnName][strings.ToLower(literalAsString)] = struct{}{}
				default:
					logger.Info().Msgf("Value is of an unexpected type: %T\n", literalAsString)
				}
			}
		}
		return NewInfixExpr(e.Left.Accept(b).(Expr), e.Op, e.Right.Accept(b).(Expr))
	}

	selectCmd.Accept(visitor)

}

// HighlightValue takes a value and returns the part of it that should be highlighted, wrapped in tags.
//
// E.g. when value is `Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 Firefox/6.0a1
// and we search for `Firefo` in Kibana it's going to produce `@kibana-highlighted-field@Firefo@/kibana-highlighted-field@`
func (h *Highlighter) HighlightValue(columnName, value string) []string {
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
	for _, token := range h.GetSortedTokens(columnName) {
		if token == "" {
			continue
		}
		pos := 0
		for pos < length { // tokens are stored as lowercase
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
