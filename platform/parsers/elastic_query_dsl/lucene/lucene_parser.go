// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package lucene

import (
	"context"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/model"
	"github.com/QuesmaOrg/quesma/platform/schema"
)

// Mainly based on this doc: https://lucene.apache.org/core/2_9_4/queryparsersyntax.html
// Alternatively: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html

// We don't support:
// - Fuzzy search (e.g. roam~0.8, ~0.8 is simply removed)
// - Wildcards ? and * - they are treated as regular characters
//   (I think I'll add at least some basic support for them quite soon, it's needed for sample dashboards)
// - escaped " inside quoted fieldnames, so e.g.
//     * "a\"b" - not supported
//     * abc"def - supported
// - +, -, &&, ||, operators. But AND, OR, NOT are fully supported and they seem equivalent.

// Date ranges are only in format YYYY-MM-DD, as in docs there are no other examples. That can be changed if needed.

// Used in parsing one Lucene query. During parsing WhereStatement keeps parsed part of the query,
// and tokens keep the rest (unparsed yet) part of the query.
// After parsing, the result statement is kept in p.WhereStatement (we should change it in the future)
// If you have multiple queries to parse, create a new luceneParser for each query.
type (
	luceneParser struct {
		ctx               context.Context
		tokens            []token
		defaultFieldNames []string
		// This is a little awkward, at some point we should remove `WhereStatement` and just return the statement from `BuildWhereStatement`
		// However, given parsing implementation, it's easier to keep it for now.
		WhereStatement model.Expr

		currentSchema schema.Schema
	}
)

func newLuceneParser(ctx context.Context, defaultFieldNames []string, currentSchema schema.Schema) luceneParser {
	return luceneParser{ctx: ctx, defaultFieldNames: defaultFieldNames, tokens: make([]token, 0), currentSchema: currentSchema}
}

const fuzzyOperator = '~'
const boostingOperator = '^'
const escapeCharacter = '\\'

const delimiterCharacter = ':'

const leftParenthesis = '('
const rightParenthesis = ')'
const inclusiveRangeOpeningCharacter = '['
const inclusiveRangeClosingCharacter = ']'
const exclusiveRangeOpeningCharacter = '{'
const exclusiveRangeClosingCharacter = '}'
const rangeSeparator = " TO "
const infiniteRange = "*"

var specialOperators = map[string]token{
	"AND ":                   andToken{},
	"OR ":                    orToken{},
	"NOT ":                   notToken{},
	"!":                      notToken{},
	"_exists_:":              existsToken{},
	string(leftParenthesis):  leftParenthesisToken{},
	string(rightParenthesis): rightParenthesisToken{},
}

func TranslateToSQL(ctx context.Context, query string, fields []string, currentSchema schema.Schema) model.Expr {
	parser := newLuceneParser(ctx, fields, currentSchema)
	return parser.translateToSQL(query)
}

func (p *luceneParser) translateToSQL(query string) model.Expr {
	query = p.removeBoostingOperator(query)
	p.tokenizeQuery(query)
	if len(p.tokens) == 1 {
		if _, isInvalidToken := p.tokens[0].(invalidToken); isInvalidToken {
			logger.WarnWithCtx(p.ctx).Msgf("Invalid query, can't tokenize: %s", query)
		}
	}
	return p.BuildWhereStatement()
}

// tokenizeQuery splits the query into tokens, which are stored in p.tokens.
// If query is invalid, p.tokens contains only one invalidToken.
func (p *luceneParser) tokenizeQuery(query string) {
	query = strings.TrimSpace(query)
	for len(query) > 0 {
		nextTokens, remainingQuery := p.nextToken(query)
		for _, tok := range nextTokens {
			if _, isInvalidToken := tok.(invalidToken); isInvalidToken {
				p.tokens = []token{newInvalidToken()}
				return
			}
		}
		p.tokens = append(p.tokens, nextTokens...)
		query = strings.TrimSpace(remainingQuery)
	}
}

func (p *luceneParser) nextToken(query string) (tokens []token, remainingQuery string) {
	// parsing special operators
	for operator, operatorToken := range specialOperators {
		if strings.HasPrefix(query, operator) {
			return []token{operatorToken}, query[len(operator):]
		}
	}

	// parsing term(:value)
	term, remainingQuery := p.parseTerm(query, false)

	// case 1. there's no ":value"
	remainingQuery = strings.TrimSpace(remainingQuery)
	if len(remainingQuery) == 0 || remainingQuery[0] != delimiterCharacter {
		return []token{term}, remainingQuery
	}

	// case 2. query[len(term)] == ':" => there's ":value"
	if termCasted, termIsFieldName := term.(termToken); termIsFieldName {
		// this branch should always be used, but being cautious and wrapping in if
		// to not panic in case of invalid query
		return []token{newFieldNameToken(termCasted.term), newSeparatorToken()}, remainingQuery[1:]
	}
	return []token{term, newSeparatorToken()}, remainingQuery[1:]
}

// query - non-empty string
// closingBoundTerm is true <=> we're parsing the second bound of the range.
// Then we finish when we encounter ']' or '}'. Otherwise we don't.
func (p *luceneParser) parseTerm(query string, closingBoundTerm bool) (token token, remainingQuery string) {
	switch query[0] {
	case '"':
		for i, r := range query[1:] {
			if r == '"' {
				term := query[:i+2]
				remainingQuery = query[i+2:]
				// Check for fuzzy operator after quoted term (e.g., "term"~2)
				if strings.HasPrefix(remainingQuery, string(fuzzyOperator)) {
					// Parse fuzzy operator from remaining query
					distanceEnd := 1 // Start after ~

					// Find where distance ends (space, delimiter, etc.)
					for distanceEnd < len(remainingQuery) {
						r := remainingQuery[distanceEnd]
						if r == ' ' || r == delimiterCharacter || r == rightParenthesis {
							break
						}
						distanceEnd++
					}

					distanceStr := remainingQuery[1:distanceEnd] // Skip the ~
					distance := p.parseFuzzyDistance(distanceStr)

					// Remove quotes from term for fuzzy search
					cleanTerm := term[1 : len(term)-1] // Remove quotes
					logger.InfoWithCtx(p.ctx).Msgf("Parsed fuzzy term: %s with distance: %d", cleanTerm, distance)
					return newFuzzyToken(cleanTerm, distance), remainingQuery[distanceEnd:]
				}
				return newTermToken(term), remainingQuery
			}
		}
		logger.Error().Msgf("unterminated quoted term, query: %s", query)
		return newInvalidToken(), ""
	case '>', '<', inclusiveRangeOpeningCharacter, exclusiveRangeOpeningCharacter:
		return p.parseRange(query)
	default:
		for i, r := range query {
			if r == ' ' || r == delimiterCharacter || r == rightParenthesis || (closingBoundTerm && (r == exclusiveRangeClosingCharacter || r == inclusiveRangeClosingCharacter)) {
				term := query[:i]
				remainingQuery = query[i:]
				// Check for fuzzy operator
				if fuzzyTok, remaining := p.parseFuzzyIfPresent(term, remainingQuery); fuzzyTok != nil {
					return fuzzyTok, remaining
				}
				return newTermToken(term), remainingQuery
			}
		}
		// End of query reached
		term := query
		remainingQuery = ""
		// Check for fuzzy operator
		if fuzzyTok, remaining := p.parseFuzzyIfPresent(term, remainingQuery); fuzzyTok != nil {
			return fuzzyTok, remaining
		}
		return newTermToken(term), remainingQuery
	}
}

// parseFuzzyIfPresent checks if the term contains fuzzy operator and parses it
// Returns fuzzy token if fuzzy operator found, nil otherwise
func (p *luceneParser) parseFuzzyIfPresent(term string, remainingQuery string) (token, string) {
	// Check if term contains fuzzy operator
	fuzzyIndex := strings.LastIndex(term, string(fuzzyOperator))
	if fuzzyIndex == -1 {
		return nil, remainingQuery
	}

	// Check if it's escaped
	if fuzzyIndex > 0 && term[fuzzyIndex-1] == escapeCharacter {
		return nil, remainingQuery
	}

	// Extract the base term (before ~)
	baseTerm := term[:fuzzyIndex]
	if baseTerm == "" {
		return nil, remainingQuery
	}

	// Extract distance (after ~)
	distanceStr := term[fuzzyIndex+1:]
	distance := p.parseFuzzyDistance(distanceStr)

	return newFuzzyToken(baseTerm, distance), remainingQuery
}

// parseFuzzyDistance converts a distance string to an integer for fuzzy search.
// Returns 2 as default if distanceStr is empty or invalid.
// For fractional values like 0.8, returns 1 as minimum distance.
func (p *luceneParser) parseFuzzyDistance(distanceStr string) int {
	if distanceStr == "" {
		return 2 // default fuzzy distance
	}

	if parsedFloat, err := strconv.ParseFloat(distanceStr, 64); err == nil && parsedFloat >= 0 {
		// Convert float to int - Elasticsearch typically uses this for edit distance
		// For fractional values like 0.8, we'll use 1 as minimum distance
		if parsedFloat < 1.0 && parsedFloat > 0 {
			return 1
		} else {
			return int(parsedFloat)
		}
	}

	return 2 // default if parsing fails
}

func (p *luceneParser) parseRange(query string) (token token, remainingQuery string) {
	var number float64
	switch query[0] {
	case '>', '<':
		if len(query) == 1 {
			logger.Error().Msgf("parseRange: invalid range, missing value, query: %s", query)
			return newInvalidToken(), ""
		}
		acceptableCharactersAfterNumber := []rune{' ', rightParenthesis}
		if query[1] == '=' { // >=, <=
			number, remainingQuery = p.parseNumber(query[2:], true, acceptableCharactersAfterNumber)
			switch query[0] {
			case '>':
				return newRangeToken(newRangeValueGte(number)), remainingQuery
			case '<':
				return newRangeToken(newRangeValueLte(number)), remainingQuery
			}
		} else {
			number, remainingQuery = p.parseNumber(query[1:], true, acceptableCharactersAfterNumber)
			switch query[0] {
			case '>':
				return newRangeToken(newRangeValueGt(number)), remainingQuery
			case '<':
				return newRangeToken(newRangeValueLt(number)), remainingQuery
			}
		}
	case inclusiveRangeOpeningCharacter, exclusiveRangeOpeningCharacter:
		var lowerBound, upperBound any
		lowerBound, remainingQuery = p.parseOneBound(query[1:], false)
		if _, isInvalid := lowerBound.(invalidToken); isInvalid {
			return newInvalidToken(), ""
		}
		if len(remainingQuery) < len(rangeSeparator) || remainingQuery[:len(rangeSeparator)] != rangeSeparator {
			return newInvalidToken(), ""
		}
		upperBound, remainingQuery = p.parseOneBound(remainingQuery[len(rangeSeparator):], true)
		if _, isInvalid := upperBound.(invalidToken); isInvalid || len(remainingQuery) == 0 {
			return newInvalidToken(), ""
		}
		inclusiveOpening := query[0] == inclusiveRangeOpeningCharacter
		inclusiveClosing := remainingQuery[0] == inclusiveRangeClosingCharacter
		return newRangeToken(newRangeValue(lowerBound, inclusiveOpening, upperBound, inclusiveClosing)), remainingQuery[1:]
	}
	logger.Error().Msgf("parseRange: invalid range, query: %s", query)
	return newInvalidToken(), ""
}

// parseNumber returns (math.NaN, "") if parsing failed
// acceptableCharsAfterNumber - what character is acceptable as first character after the number,
// e.g. when acceptableCharsAfterNumber = {']', '}'}, then 200} or 200] parses to 200, but parsing 200( fails.
func (p *luceneParser) parseNumber(query string, reportErrors bool, acceptableCharsAfterNumber []rune) (number float64, remainingQuery string) {
	query = strings.TrimSpace(query)
	i, dotCount := 0, 0
	if len(query) > 0 && query[0] == '-' {
		i++
	}
	for ; i < len(query); i++ {
		r := rune(query[i])
		if r == '.' {
			dotCount++
			if dotCount > 1 {
				if reportErrors {
					logger.Error().Msgf("invalid number, multiple dots, query: %s", query)
				}
				return math.NaN(), ""
			}
			continue
		}
		if !unicode.IsDigit(r) {
			if !slices.Contains(acceptableCharsAfterNumber, r) {
				if reportErrors {
					logger.Error().Msgf("invalid number, query: %s", query)
				}
				return math.NaN(), ""
			}
			break
		}
	}
	var err error
	number, err = strconv.ParseFloat(query[:i], 64)
	if err != nil {
		if reportErrors {
			logger.Error().Msgf("invalid number, query: %s, error: %v", query, err)
		}
		return math.NaN(), ""
	}
	return number, query[i:]
}

// parseOneBound returns invalidToken{} if parsing failed
// closingBound == true <=> it's second bound, so ] or } are totally fine after the number
func (p *luceneParser) parseOneBound(query string, closingBound bool) (bound any, remainingQuery string) {
	// let's try parsing a number first, only if it fails, we'll parse it as a string
	var acceptableCharactersAfterNumber []rune
	if closingBound {
		acceptableCharactersAfterNumber = []rune{inclusiveRangeClosingCharacter, exclusiveRangeClosingCharacter}
	} else {
		acceptableCharactersAfterNumber = []rune{' '}
	}
	var number float64
	number, remainingQuery = p.parseNumber(query, false, acceptableCharactersAfterNumber)
	if !math.IsNaN(number) {
		return number, remainingQuery
	}

	var tok token
	tok, remainingQuery = p.parseTerm(query, closingBound)
	if term, isTerm := tok.(termToken); isTerm {
		if term.term == infiniteRange {
			bound = unbounded
		} else {
			bound = term.term
		}
		return bound, remainingQuery
	} else {
		logger.Error().Msgf("parseRange: invalid range, query: %s", query)
		return newInvalidToken(), ""
	}
}

func (p *luceneParser) removeFuzzySearchOperator(query string) string {
	return p.removeSpecialCharacter(query, fuzzyOperator)
}

func (p *luceneParser) removeBoostingOperator(query string) string {
	return p.removeSpecialCharacter(query, boostingOperator)
}

func (p *luceneParser) removeSpecialCharacter(query string, specialChar byte) string {
	var afterRemoval strings.Builder
	for i := 0; i < len(query); i++ {
		if query[i] == escapeCharacter && i+1 < len(query) && query[i+1] == specialChar {
			// it's escaped, we don't remove it
			i++
		} else if query[i] == specialChar {
			// remove the character together with the following number (may be float)
			for ; i+1 < len(query) && unicode.IsDigit(rune(query[i+1])); i++ {
			}
			if i+1 < len(query) && query[i+1] == '.' {
				i++
				for ; i+1 < len(query) && unicode.IsDigit(rune(query[i+1])); i++ {
				}
			}
		} else {
			afterRemoval.WriteByte(query[i])
		}
	}
	return afterRemoval.String()
}
