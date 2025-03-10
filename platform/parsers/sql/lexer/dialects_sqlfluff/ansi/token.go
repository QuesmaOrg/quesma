// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// This file contains code derived from:
//
// 1. sqlfluff (Copyright (c) 2023 Alan Cruickshank)
//    Licensed under MIT License
//    https://github.com/sqlfluff/sqlfluff/blob/6666db9ed97f45161fb318f901392d9a214808d2/LICENSE.md

package ansi

import "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"

// The following code block is based on sqlfluff:
// https://github.com/sqlfluff/sqlfluff/blob/6666db9ed97f45161fb318f901392d9a214808d2/src/sqlfluff/dialects/dialect_ansi.py

var WhitespaceTokenType = core.TokenType{
	Name:        "WhitespaceSegment",
	Description: "Whitespace segment",
}

var CommentTokenType = core.TokenType{
	Name:        "CommentSegment",
	Description: "Comment segment",
}

var CodeTokenType = core.TokenType{
	Name:        "CodeSegment",
	Description: "Code segment",
}

var LiteralTokenType = core.TokenType{
	Name:        "LiteralSegment",
	Description: "Literal segment",
}

var ComparisonOperatorTokenType = core.TokenType{
	Name:        "ComparisonOperatorSegment",
	Description: "Comparison operator segment",
}

var NewlineTokenType = core.TokenType{
	Name:        "NewlineSegment",
	Description: "Newline segment",
}

var WordTokenType = core.TokenType{
	Name:        "WordSegment",
	Description: "Word segment",
}
