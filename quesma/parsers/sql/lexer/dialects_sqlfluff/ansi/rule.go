// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// This file contains code derived from:
//
// 1. sqlfluff (Copyright (c) 2023 Alan Cruickshank)
//    Licensed under MIT License
//    https://github.com/sqlfluff/sqlfluff/blob/6666db9ed97f45161fb318f901392d9a214808d2/LICENSE.md

package ansi

import (
	"github.com/QuesmaOrg/quesma/quesma/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/quesma/parsers/sql/lexer/dialect_sqlparse"
)

// The following code block is based on sqlfluff:
// https://github.com/sqlfluff/sqlfluff/blob/6666db9ed97f45161fb318f901392d9a214808d2/src/sqlfluff/dialects/dialect_ansi.py

var ansiRules = []core.Rule{
	core.NewRegexRule(`[^\S\r\n]+`, &WhitespaceTokenType),
	core.NewRegexRule(`(--|#)[^\n]*`, &CommentTokenType),

	// MODIFICATION: original regex doesn't compile in golang:
	// core.NewRegexRule(`\/\*([^\*]|\*(?!\/))*\*\/`, &CommentTokenType),
	// replacing it with regex from dialect_sqlparse:
	core.NewRegexRule(`/\*[\s\S]*?\*/`, &CommentTokenType),

	core.NewRegexRule(`'([^'\\]|\\.|'')*'`, &CodeTokenType),
	core.NewRegexRule(`"(""|[^"\\]|\\.)*"`, &CodeTokenType),
	core.NewRegexRule("`(?:[^`\\\\]|\\\\.)*`", &CodeTokenType),

	// MODIFICATION: original regex doesn't compile in golang:
	// core.NewRegexRule(`\$(\w*)\$(.*?)\$\1\$`, &CodeTokenType),
	dialect_sqlparse.NewDollarQuoteRule(`\$(\w*)\$`, `(.*?)`, &CodeTokenType),

	// MODIFICATION: original regex doesn't compile in golang:
	// core.NewRegexRule(`(?>\d+\.\d+|\d+\.(?![\.\w])|\.\d+|\d+)(\.?[eE][+-]?\d+)?((?<=\.)|(?=\b))`, &LiteralTokenType),
	// replacing it with (modified) number rules from dialect_sqlparse:
	core.NewRegexRule(`(\d+\.\d*|\d*\.\d+|\d+)E[+-]?\d+`, &LiteralTokenType),
	dialect_sqlparse.NewNegativeLookaheadRule(`(\d+(\.\d*)|\.\d+)`, `_A-ZÀ-Ü\d`, &LiteralTokenType),
	dialect_sqlparse.NewNegativeLookaheadRule(`\d+`, `_A-ZÀ-Ü\d`, &LiteralTokenType),

	core.NewRegexRule(`!?~~?\*?`, &ComparisonOperatorTokenType),
	// MODIFICATION: original regex:
	// core.NewRegexRule(`\r\n|\n`, &NewlineTokenType),
	core.NewRegexRule(`(\r\n|\r|\n)`, &NewlineTokenType),

	core.NewStringRule(`::`, &CodeTokenType),
	core.NewStringRule(`=`, &CodeTokenType),
	core.NewStringRule(`>`, &CodeTokenType),
	core.NewStringRule(`<`, &CodeTokenType),
	core.NewStringRule(`!`, &CodeTokenType),
	core.NewStringRule(`.`, &CodeTokenType),
	core.NewStringRule(`,`, &CodeTokenType),
	core.NewStringRule(`+`, &CodeTokenType),
	core.NewStringRule(`-`, &CodeTokenType),
	core.NewStringRule(`/`, &CodeTokenType),
	core.NewStringRule(`%`, &CodeTokenType),
	core.NewStringRule(`?`, &CodeTokenType),
	core.NewStringRule(`&`, &CodeTokenType),
	core.NewStringRule(`|`, &CodeTokenType),
	core.NewStringRule(`^`, &CodeTokenType),
	core.NewStringRule(`*`, &CodeTokenType),
	core.NewStringRule(`(`, &CodeTokenType),
	core.NewStringRule(`)`, &CodeTokenType),
	core.NewStringRule(`[`, &CodeTokenType),
	core.NewStringRule(`]`, &CodeTokenType),
	core.NewStringRule(`{`, &CodeTokenType),
	core.NewStringRule(`}`, &CodeTokenType),
	core.NewStringRule(`:`, &CodeTokenType),
	core.NewStringRule(`;`, &CodeTokenType),
	core.NewRegexRule(`[0-9a-zA-Z_]+`, &WordTokenType),
}

var SqlfluffAnsiRules = core.NewRuleList(ansiRules...)
