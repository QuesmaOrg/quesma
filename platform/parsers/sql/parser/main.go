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
	tokens := lexer_core.Lex("FROM kibana_sample_data_logs |> SELECT `@timestamp` |> CALL TIMEBUCKET `@timestamp` BY 3 HOUR AS timebucket |> AGGREGATE count(*) AS logs_count GROUP BY timebucket |> ORDER BY timebucket", dialect_sqlparse.SqlparseRules)

	//tokens := lexer_core.Lex(`SELECT * FROM tabela WHERE b = 9 |> JOIN tabela2 ON b = d |> WHERE b = 3 |> ORDER BY b |> WHERE d = 9 |> SELECT a, b, c |> LIMIT 100`, dialect_sqlparse.SqlparseRules)
	node := core.TokensToNode(tokens)

	transforms.GroupParenthesis(node)
	pipe_syntax.GroupPipeSyntax(node)
	pipe_syntax.ExpandMacros(node)
	pipe_syntax.Transpile(node)

	fmt.Println(transforms.ConcatTokenNodes(node))
}
