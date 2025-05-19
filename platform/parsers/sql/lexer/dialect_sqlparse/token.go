// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// This file contains code derived from:
//
// 1. sqlparse (Copyright (c) 2016, Andi Albrecht)
//    Licensed under BSD-3-Clause License
//    https://github.com/andialbrecht/sqlparse/blob/38c065b86ac43f76ffd319747e57096ed78bfa63/LICENSE

package dialect_sqlparse

import "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"

// The following code block is based on sqlparse:
// https://github.com/andialbrecht/sqlparse/blob/38c065b86ac43f76ffd319747e57096ed78bfa63/sqlparse/keywords.py

var KeywordTokenType = core.TokenType{
	Name:        "Token.Keyword",
	Description: "General keyword token",
}

var DMLKeywordTokenType = core.TokenType{
	Name:        "Token.Keyword.DML",
	Description: "DML keyword token (e.g. SELECT, INSERT, UPDATE, DELETE)",
}

var DDLKeywordTokenType = core.TokenType{
	Name:        "Token.Keyword.DDL",
	Description: "DDL keyword token (e.g. DROP, CREATE, ALTER)",
}

var DCLKeywordTokenType = core.TokenType{
	Name:        "Token.Keyword.DCL",
	Description: "DCL keyword token (e.g. GRANT, REVOKE)",
}

var CTEKeywordTokenType = core.TokenType{
	Name:        "Token.Keyword.CTE",
	Description: "CTE keyword token (e.g. WITH)",
}

var OrderKeywordTokenType = core.TokenType{
	Name:        "Token.Keyword.Order",
	Description: "Order keyword token (e.g. ASC, DESC)",
}

var TZCastKeywordTokenType = core.TokenType{
	Name:        "Token.Keyword.TZCast",
	Description: "Timezone cast keyword token (e.g. AT TIME ZONE)",
}

var OperatorTokenType = core.TokenType{
	Name:        "Token.Operator",
	Description: "Operator token (e.g. DIV)",
}

var ComparisonOperatorTokenType = core.TokenType{
	Name:        "Token.Operator.Comparison",
	Description: "Comparison operator token (e.g. =, <, >)",
}

var NameTokenType = core.TokenType{
	Name:        "Token.Name",
	Description: "Name token",
}

var BuiltinNameTokenType = core.TokenType{
	Name:        "Token.Name.Builtin",
	Description: "Builtin token (e.g. ARRAY, BIGINT, BINARY)",
}

var PlaceholderNameTokenType = core.TokenType{
	Name:        "Token.Name.Placeholder",
	Description: "Placeholder token",
}

var SingleCommentTokenType = core.TokenType{
	Name:        "Token.Comment.Single",
	Description: "Single-line comment token",
}

var MultilineCommentTokenType = core.TokenType{
	Name:        "Token.Comment.Multiline",
	Description: "Multiline comment token",
}

var SingleHintTokenType = core.TokenType{
	Name:        "Token.Comment.Single.Hint",
	Description: "Single-line hint token",
}

var MultilineHintTokenType = core.TokenType{
	Name:        "Token.Comment.Multiline.Hint",
	Description: "Multiline hint token",
}

var NewlineTokenType = core.TokenType{
	Name:        "Token.Text.Whitespace.Newline",
	Description: "Newline token",
}

var WhitespaceTokenType = core.TokenType{
	Name:        "Token.Text.Whitespace",
	Description: "Whitespace token",
}

var AssignmentTokenType = core.TokenType{
	Name:        "Token.Assignment",
	Description: "Assignment token",
}

var PunctuationTokenType = core.TokenType{
	Name:        "Token.Punctuation",
	Description: "Punctuation token (e.g. ;, :, ::, (, ), )",
}

var WildcardTokenType = core.TokenType{
	Name:        "Token.Wildcard",
	Description: "Wildcard token (* character)",
}

var LiteralTokenType = core.TokenType{
	Name:        "Token.Literal",
	Description: "Literal token",
}

var CommandTokenType = core.TokenType{
	Name:        "Token.Generic.Command",
	Description: "Command token",
}

var IntegerNumberTokenType = core.TokenType{
	Name:        "Token.Literal.Number.Integer",
	Description: "Integer number token",
}

var HexadecimalNumberTokenType = core.TokenType{
	Name:        "Token.Literal.Number.Hexadecimal",
	Description: "Hexadecimal number token",
}

var FloatNumberTokenType = core.TokenType{
	Name:        "Token.Literal.Number.Float",
	Description: "Float number token",
}

var SingleStringTokenType = core.TokenType{
	Name:        "Token.Literal.String.Single",
	Description: "Single-quoted string token",
}

var SymbolStringTokenType = core.TokenType{
	Name:        "Token.Literal.String.Symbol",
	Description: "Symbol string token",
}

var ErrorTokenType = core.TokenType{
	Name:        "Token.Error",
	Description: "Error token. The input at this position did not match any rule, so this character was skipped",
}

var PipeTokenType = core.TokenType{
	Name:        "Token.Keyword.Pipe",
	Description: "SQL with pipe syntax operator |>",
}
