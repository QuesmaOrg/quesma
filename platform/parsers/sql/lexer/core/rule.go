// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// This file contains code derived from:
//
// 1. sqlfluff (Copyright (c) 2023 Alan Cruickshank)
//    Licensed under MIT License
//    https://github.com/sqlfluff/sqlfluff/blob/6666db9ed97f45161fb318f901392d9a214808d2/LICENSE.md

package core

import (
	"regexp"
	"strings"
)

type Rule interface {
	Match(input string, position int) (Token, bool)
	Name() string
}

// The following code block is based on RegexLexer from sqlfluff:
// https://github.com/sqlfluff/sqlfluff/blob/6666db9ed97f45161fb318f901392d9a214808d2/src/sqlfluff/core/parser/lexer.py#L308
type RegexRule struct {
	regex              *regexp.Regexp
	resultingTokenType *TokenType
}

func NewRegexRule(regex string, resultingTokenType *TokenType) *RegexRule {
	return &RegexRule{regex: regexp.MustCompile(`^(?is)` + regex), resultingTokenType: resultingTokenType}
}

func (r *RegexRule) Match(input string, position int) (Token, bool) {
	match := r.regex.FindString(input[position:])
	if len(match) == 0 {
		return EmptyToken, false
	}

	return MakeToken(position, match, r.resultingTokenType), true
}

func (r *RegexRule) Name() string {
	return "RegexRule"
}

// The following code block is based on StringLexer from sqlfluff:
// https://github.com/sqlfluff/sqlfluff/blob/6666db9ed97f45161fb318f901392d9a214808d2/src/sqlfluff/core/parser/lexer.py#L128
type StringRule struct {
	pattern            string
	resultingTokenType *TokenType
}

func NewStringRule(pattern string, resultingTokenType *TokenType) *StringRule {
	return &StringRule{pattern: strings.ToUpper(pattern), resultingTokenType: resultingTokenType}
}

func (s *StringRule) Match(input string, position int) (Token, bool) {
	// FIXME: improve performance, avoiding ToUpper on entire input
	if !strings.HasPrefix(strings.ToUpper(input[position:]), s.pattern) {
		return EmptyToken, false
	}

	return MakeToken(position, input[position:position+len(s.pattern)], s.resultingTokenType), true
}

func (s *StringRule) Name() string {
	return "StringRule"
}

type RuleList struct {
	rules []Rule
}

func NewRuleList(rules ...Rule) *RuleList {
	return &RuleList{rules: rules}
}

func (r RuleList) Match(input string, position int) (Token, bool) {
	for _, rule := range r.rules {
		token, matched := rule.Match(input, position)
		if matched {
			return token, true
		}
	}

	return EmptyToken, false
}

func (r RuleList) Name() string {
	return "RuleList"
}
