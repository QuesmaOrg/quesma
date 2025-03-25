// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import (
	"fmt"
	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/dialect_sqlparse"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/pipe_syntax"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/transforms"
)

func main() {
	tokens := lexer_core.Lex(
		`
from akamai.siem
|> LEFT JOIN x ON x.id = siem.id
|> limit 100 
`, dialect_sqlparse.SqlparseRules)

	node := core.TokensToNode(tokens)

	transforms.GroupParenthesis(node)
	pipe_syntax.GroupPipeSyntax(node)
	pipe_syntax.ExpandMacros(node)
	pipe_syntax.Transpile(node)

	fmt.Println(transforms.ConcatTokenNodes(node))
}
