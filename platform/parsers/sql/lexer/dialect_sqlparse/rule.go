// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// This file contains code derived from:
//
// 1. sqlparse (Copyright (c) 2016, Andi Albrecht)
//    Licensed under BSD-3-Clause License
//    https://github.com/andialbrecht/sqlparse/blob/38c065b86ac43f76ffd319747e57096ed78bfa63/LICENSE

package dialect_sqlparse

import (
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"regexp"
	"strings"
)

// The following code block is based on SQL_REGEX from sqlparse:
// https://github.com/andialbrecht/sqlparse/blob/38c065b86ac43f76ffd319747e57096ed78bfa63/sqlparse/keywords.py
// See the linked file for the original comments

// MODIFICATION: All instances of \w were replaced with [\pL\d_] to support Unicode
var SQL_REGEX = []core.Rule{
	core.NewRegexRule(`(--|# )\+.*?(\r\n|\r|\n|$)`, &SingleHintTokenType),
	core.NewRegexRule(`/\*\+[\s\S]*?\*/`, &MultilineHintTokenType),

	core.NewRegexRule(`(--|# ).*?(\r\n|\r|\n|$)`, &SingleCommentTokenType),
	core.NewRegexRule(`/\*[\s\S]*?\*/`, &MultilineCommentTokenType),

	core.NewRegexRule(`(\r\n|\r|\n)`, &NewlineTokenType),
	core.NewRegexRule(`\s+?`, &WhitespaceTokenType),

	core.NewRegexRule(`:=`, &AssignmentTokenType),
	core.NewRegexRule(`::`, &PunctuationTokenType),

	core.NewRegexRule(`\*`, &WildcardTokenType),

	core.NewRegexRule("`(``|[^`])*`", &NameTokenType),
	core.NewRegexRule(`´(´´|[^´])*´`, &NameTokenType),

	// Dollar quoting: https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING
	//
	// For example:
	// $$SELECT * FROM table$$
	// or
	// $MYQUOTE$SELECT * FROM table$MYQUOTE$
	//
	// It's commonly used in CREATE FUNCTION statements in PostgreSQL.
	//
	// Go's regexp package doesn't support lookbehind and backreferences, so we reimplement the original regex:
	// core.NewRegexRule(`((?<![\pL\d_\"\$])\$(?:[_A-ZÀ-Ü][\pL\d_]*)?\$)[\s\S]*?\1`, &LiteralTokenType),
	//
	// Splitting the original regex into parts:
	// 1. (?<![\pL\d_\"\$])
	//    Negative Lookbehind: text behind us can't end at [\pL\d_\"\$] character
	// 2.1. \$
	// 2.2  (?:[_A-ZÀ-Ü][\pL\d_]*)?
	//      potential string (e.g. MYQUOTE in example above)
	// 2.3. \$
	// 3. [\s\S]*?
	//    inner contents
	// 4. \1
	//    matching quote start (e.g. $$ or $MYQUOTE$, must be identical to the first part)
	//
	// MODIFICATION: Original regex:
	// core.NewRegexRule(`((?<![\pL\d_\"\$])\$(?:[_A-ZÀ-Ü][\pL\d_]*)?\$)[\s\S]*?\1`, &LiteralTokenType),
	NewNegativeLookbehindRule(`(?:[\pL\d_\"\$])`,
		NewDollarQuoteRule(`(\$(?:[_A-ZÀ-Ü][\pL\d_]*)?\$)`, `[\s\S]*?`, &LiteralTokenType)),

	core.NewRegexRule(`\?`, &PlaceholderNameTokenType),
	core.NewRegexRule(`%(\([\pL\d_]+\))?s`, &PlaceholderNameTokenType),

	// MODIFICATION: Original regex:
	//core.NewRegexRule(`(?<![\pL\d_])[$:?][\pL\d_]+`, &PlaceholderNameTokenType),
	NewNegativeLookbehindRule(`[\pL\d_]`, core.NewRegexRule(`[$:?][\pL\d_]+`, &PlaceholderNameTokenType)),

	core.NewRegexRule(`\\[\pL\d_]+`, &CommandTokenType),

	core.NewRegexRule(`(CASE|IN|VALUES|USING|FROM|AS)\b`, &KeywordTokenType),

	core.NewRegexRule(`(@|##|#)[A-ZÀ-Ü][\pL\d_]+`, &NameTokenType),

	// MODIFICATION: Original regex:
	// core.NewRegexRule(`[A-ZÀ-Ü][\pL\d_]*(?=\s*\.)`, &NameTokenType), // 'Name'.
	NewPositiveLookaheadRule(`[A-ZÀ-Ü][\pL\d_]*`, `\s*\.`, &NameTokenType), // 'Name'.

	// MODIFICATION: Original regex:
	//core.NewRegexRule(`(?<=\.)[A-ZÀ-Ü][\pL\d_]*`, &NameTokenType), // .'Name'
	NewPositiveLookbehindRule(`\.`, core.NewRegexRule(`[A-ZÀ-Ü][\pL\d_]*`, &NameTokenType)), // .'Name'

	// MODIFICATION: Original regex:
	// core.NewRegexRule(`[A-ZÀ-Ü][\pL\d_]*(?=\()`, &NameTokenType), // side effect: change kw to func
	NewPositiveLookaheadRule(`[A-ZÀ-Ü][\pL\d_]*`, `\(`, &NameTokenType), // side effect: change kw to func

	core.NewRegexRule(`-?0x[\dA-F]+`, &HexadecimalNumberTokenType),
	core.NewRegexRule(`-?\d+(\.\d+)?E-?\d+`, &FloatNumberTokenType),

	// MODIFICATION: Original regex:
	// core.NewRegexRule(`(?![_A-ZÀ-Ü])-?(\d+(\.\d*)|\.\d+)(?![_A-ZÀ-Ü])`, &FloatNumberTokenType),
	// I think that the first negative lookahead is not necessary. For now, removing it.
	NewNegativeLookaheadRule(`-?(\d+(\.\d*)|\.\d+)`, `_A-ZÀ-Ü`, &FloatNumberTokenType),

	// MODIFICATION: Original regex:
	// core.NewRegexRule(`(?![_A-ZÀ-Ü])-?\d+(?![_A-ZÀ-Ü])`, &IntegerNumberTokenType),
	// I think that the first negative lookahead is not necessary. For now, removing it.
	NewNegativeLookaheadRule(`-?\d+`, `_A-ZÀ-Ü`, &IntegerNumberTokenType),

	core.NewRegexRule(`'(''|\\'|[^'])*'`, &SingleStringTokenType),
	core.NewRegexRule(`"(""|\\"|[^"])*"`, &SymbolStringTokenType),
	core.NewRegexRule(`(""|".*?[^\\]")`, &SymbolStringTokenType),

	// MODIFICATION: Original regex:
	//core.NewRegexRule(`(?<![\pL\d_\])])(\[[^\]\[]+\])`, &NameTokenType),
	NewNegativeLookbehindRule(`[\pL\d_\])]`, core.NewRegexRule(`(\[[^\]\[]+\])`, &NameTokenType)),

	core.NewRegexRule(`((LEFT\s+|RIGHT\s+|FULL\s+)?(INNER\s+|OUTER\s+|STRAIGHT\s+)?`+
		`|(CROSS\s+|NATURAL\s+)?)?JOIN\b`, &KeywordTokenType),
	core.NewRegexRule(`END(\s+IF|\s+LOOP|\s+WHILE)?\b`, &KeywordTokenType),
	core.NewRegexRule(`NOT\s+NULL\b`, &KeywordTokenType),
	core.NewRegexRule(`(ASC|DESC)(\s+NULLS\s+(FIRST|LAST))?\b`, &OrderKeywordTokenType),
	core.NewRegexRule(`(ASC|DESC)\b`, &OrderKeywordTokenType),
	core.NewRegexRule(`NULLS\s+(FIRST|LAST)\b`, &OrderKeywordTokenType),
	core.NewRegexRule(`UNION\s+ALL\b`, &KeywordTokenType),
	core.NewRegexRule(`CREATE(\s+OR\s+REPLACE)?\b`, &DDLKeywordTokenType),
	core.NewRegexRule(`DOUBLE\s+PRECISION\b`, &BuiltinNameTokenType),
	core.NewRegexRule(`GROUP\s+BY\b`, &KeywordTokenType),
	core.NewRegexRule(`ORDER\s+BY\b`, &KeywordTokenType),
	core.NewRegexRule(`PRIMARY\s+KEY\b`, &KeywordTokenType),
	core.NewRegexRule(`HANDLER\s+FOR\b`, &KeywordTokenType),
	core.NewRegexRule(`GO(\s\d+)\b`, &KeywordTokenType),
	core.NewRegexRule(`(LATERAL\s+VIEW\s+)`+
		`(EXPLODE|INLINE|PARSE_URL_TUPLE|POSEXPLODE|STACK)\b`, &KeywordTokenType),
	core.NewRegexRule(`(AT|WITH')\s+TIME\s+ZONE\s+'[^']+'`, &TZCastKeywordTokenType),
	core.NewRegexRule(`(NOT\s+)?(LIKE|ILIKE|RLIKE)\b`, &ComparisonOperatorTokenType),
	core.NewRegexRule(`(NOT\s+)?(REGEXP)\b`, &ComparisonOperatorTokenType),

	NewProcessAsKeywordRule(`[\pL\d_][$#\pL\d_]*`, &NameTokenType, ALL_KEYWORDS),

	core.NewRegexRule(`[;:()\[\],\.]`, &PunctuationTokenType),

	// MODIFICATION: New rule for SQL with pipe syntax (|>)
	// Negative lookahead rule to make sure this doesn't match |>>
	// (PostGIS |>> operator: https://postgis.net/docs/ST_Geometry_Above.html)
	// FIXME: this shouldn't be in "vanilla" dialect_sqlparse, but in a separate dialect
	NewNegativeLookaheadRule(`\|>`, `>`, &PipeTokenType),

	core.NewRegexRule(`(\->>?|#>>?|@>|<@|\?\|?|\?&|\-|#\-)`, &OperatorTokenType),
	core.NewRegexRule(`[<>=~!]+`, &ComparisonOperatorTokenType),
	core.NewRegexRule(`[+/@#%^&|^-]+`, &OperatorTokenType),

	// MODIFICATION: to match error behavior in sqlparse:
	core.NewRegexRule(`.`, &ErrorTokenType),
}

var SqlparseRules = core.NewRuleList(SQL_REGEX...)

// PROCESS_AS_KEYWORD in sqlparse
type ProcessAsKeywordRule struct {
	regexRule *core.RegexRule
	keywords  map[string]*core.TokenType
}

func NewProcessAsKeywordRule(regex string, defaultTokenType *core.TokenType, keywords map[string]*core.TokenType) *ProcessAsKeywordRule {
	// TODO: add safeguard to ensure that keywords are uppercase
	return &ProcessAsKeywordRule{regexRule: core.NewRegexRule(regex, defaultTokenType), keywords: keywords}
}

func (p *ProcessAsKeywordRule) Match(input string, position int) (core.Token, bool) {
	token, matched := p.regexRule.Match(input, position)
	if matched {
		if keywordTokenType, found := p.keywords[strings.ToUpper(token.RawValue)]; found {
			token.Type = keywordTokenType
		}
	}
	return token, matched
}

func (p *ProcessAsKeywordRule) Name() string {
	return "ProcessAsKeywordRule"
}

// Golang's regexp package doesn't support positive lookbehind, so we implement it manually
// Equivalent of Python's (?<=...) regex
type PositiveLookbehindRule struct {
	regex     *regexp.Regexp
	innerRule core.Rule
}

func NewPositiveLookbehindRule(regex string, innerRule core.Rule) *PositiveLookbehindRule {
	return &PositiveLookbehindRule{regex: regexp.MustCompile(`(?is)` + regex + "$"), innerRule: innerRule}
}

func (p *PositiveLookbehindRule) Match(input string, position int) (core.Token, bool) {
	// If (positive) lookbehind regex doesn't match, the rule doesn't fire
	match := p.regex.FindString(input[:position])
	if len(match) == 0 {
		return core.EmptyToken, false
	}

	return p.innerRule.Match(input, position)
}

func (p *PositiveLookbehindRule) Name() string {
	return "PositiveLookbehindRule"
}

// Golang's regexp package doesn't support negative lookbehind, so we implement it manually
// Equivalent of Python's (?<!...) regex
type NegativeLookbehindRule struct {
	regex     *regexp.Regexp
	innerRule core.Rule
}

func NewNegativeLookbehindRule(regex string, innerRule core.Rule) *NegativeLookbehindRule {
	return &NegativeLookbehindRule{regex: regexp.MustCompile(`(?is)` + regex + "$"), innerRule: innerRule}
}

func (n *NegativeLookbehindRule) Match(input string, position int) (core.Token, bool) {
	// If (negative) lookbehind regex matches, the rule doesn't fire
	match := n.regex.FindString(input[:position])
	if len(match) != 0 {
		return core.EmptyToken, false
	}

	return n.innerRule.Match(input, position)
}

func (n *NegativeLookbehindRule) Name() string {
	return "NegativeLookbehindRule"
}

// Golang's regexp package doesn't support positive lookahead, so we implement it manually
// by transforming the regex in the following way:
//
//	regex1(?=regex2) -> (regex1)(?:regex2)
//
// and discarding the second group in the resulting match.
//
// Equivalent of Python's (?=...) regex
type PositiveLookaheadRule struct {
	regex              *regexp.Regexp
	resultingTokenType *core.TokenType
}

func NewPositiveLookaheadRule(regex string, positiveLookaheadRegex string, resultingTokenType *core.TokenType) *PositiveLookaheadRule {
	return &PositiveLookaheadRule{regex: regexp.MustCompile(`^(?is)(` + regex + `)(?:` + positiveLookaheadRegex + `)`), resultingTokenType: resultingTokenType}
}

func (r *PositiveLookaheadRule) Match(input string, position int) (core.Token, bool) {
	matches := r.regex.FindStringSubmatch(input[position:])
	if len(matches) < 2 || len(matches[1]) == 0 {
		return core.EmptyToken, false
	}
	return core.MakeToken(position, matches[1], r.resultingTokenType), true
}

func (r *PositiveLookaheadRule) Name() string {
	return "PositiveLookaheadRule"
}

// Golang's regexp package doesn't support negative lookahead, so we implement it manually
// by transforming the regex in the following way:
//
//	regex(?![alternatives]) -> (regex)(?:[^alternatives]|$)
//
// Equivalent of Python's (?!...) regex
type NegativeLookaheadRule struct {
	regex              *regexp.Regexp
	resultingTokenType *core.TokenType
}

func NewNegativeLookaheadRule(regex string, alternatives string, resultingTokenType *core.TokenType) *NegativeLookaheadRule {
	return &NegativeLookaheadRule{regex: regexp.MustCompile(`^(?is)(` + regex + `)(?:[^` + alternatives + `]|$)`), resultingTokenType: resultingTokenType}
}

func (r *NegativeLookaheadRule) Match(input string, position int) (core.Token, bool) {
	matches := r.regex.FindStringSubmatch(input[position:])
	if len(matches) < 2 || len(matches[1]) == 0 {
		return core.EmptyToken, false
	}
	return core.MakeToken(position, matches[1], r.resultingTokenType), true
}

func (r *NegativeLookaheadRule) Name() string {
	return "NegativeLookaheadRule"
}

type DollarQuoteRule struct {
	quoteStartRegex    *regexp.Regexp
	quoteInnerRegex    string
	resultingTokenType *core.TokenType
}

func NewDollarQuoteRule(quoteStartRegex string, quoteInnerRegex string, resultingTokenType *core.TokenType) *DollarQuoteRule {
	return &DollarQuoteRule{quoteStartRegex: regexp.MustCompile(`^(?is)` + quoteStartRegex),
		quoteInnerRegex: quoteInnerRegex, resultingTokenType: resultingTokenType}
}

func (r *DollarQuoteRule) Match(input string, position int) (core.Token, bool) {
	// First try to match dollar quote start (e.g. $MYQUOTE$)
	match := r.quoteStartRegex.FindString(input[position:])
	if len(match) == 0 {
		return core.EmptyToken, false
	}

	// We found $MYQUOTE$, now let's find $MYQUOTE$quoteInnerRegex$MYQUOTE$
	match = regexp.QuoteMeta(match)
	fullRegex, err := regexp.Compile(`^(?is)` + match + r.quoteInnerRegex + match)
	if err != nil {
		return core.EmptyToken, false
	}
	fullMatch := fullRegex.FindString(input[position:])
	if len(fullMatch) == 0 {
		return core.EmptyToken, false
	}

	return core.MakeToken(position, fullMatch, r.resultingTokenType), true
}

func (r *DollarQuoteRule) Name() string {
	return "DollarQuoteRule"
}
