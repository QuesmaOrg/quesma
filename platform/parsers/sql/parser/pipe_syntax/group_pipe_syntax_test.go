// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package pipe_syntax

import (
	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/dialect_sqlparse"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/transforms"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGroupPipeSyntaxSimple(t *testing.T) {
	tokens := lexer_core.Lex(
		`from tabela |> SELECT * |> where a > 5 |> where c = '|>' |> order by c DESC`, dialect_sqlparse.SqlparseRules)

	node := core.TokensToNode(tokens)

	transforms.GroupParenthesis(node)
	GroupPipeSyntax(node)

	nodeListNode, ok := node.(*core.NodeListNode)
	assert.True(t, ok)
	assert.Equal(t, 1, len(nodeListNode.Nodes))

	pipeNode, ok := nodeListNode.Nodes[0].(*PipeNode)
	assert.True(t, ok)

	assert.Equal(t, "from tabela ", transforms.ConcatTokenNodes(pipeNode.BeforePipe))

	assert.Equal(t, 4, len(pipeNode.Pipes))
	assert.Equal(t, "|> SELECT * ", transforms.ConcatTokenNodes(pipeNode.Pipes[0]))
	assert.Equal(t, "|> where a > 5 ", transforms.ConcatTokenNodes(pipeNode.Pipes[1]))
	assert.Equal(t, "|> where c = '|>' ", transforms.ConcatTokenNodes(pipeNode.Pipes[2]))
	assert.Equal(t, "|> order by c DESC", transforms.ConcatTokenNodes(pipeNode.Pipes[3]))
}

//func TestGroupPipeSyntaxNested(t *testing.T) {
//	tokens := lexer_core.Lex(
//		`from tabela |> SELECT * |> JOIN (SELECT 1 as id, "test" as name |> WHERE id = 1) AS tabela2 ON tabela.id = tabela2.id`, dialect_sqlparse.SqlparseRules)
//
//	node := core.TokensToNode(tokens)
//
//	transforms.GroupParenthesis(node)
//	GroupPipeSyntax(node)
//
//	nodeListNode, ok := node.(*core.NodeListNode)
//	assert.True(t, ok)
//	assert.Equal(t, 1, len(nodeListNode.Nodes))
//
//	pipeNode, ok := nodeListNode.Nodes[0].(*PipeNode)
//	assert.True(t, ok)
//
//	assert.Equal(t, "from tabela ", transforms.ConcatTokenNodes(pipeNode.BeforePipe))
//
//	assert.Equal(t, 2, len(pipeNode.Pipes))
//	assert.Equal(t, "|> SELECT * ", transforms.ConcatTokenNodes(pipeNode.Pipes[0]))
//	assert.Equal(t, `|> JOIN (SELECT 1 as id, "test" as name |> WHERE id = 1) AS tabela2 ON tabela.id = tabela2.id`, transforms.ConcatTokenNodes(pipeNode.Pipes[1]))
//
//	innerNodeListNode, ok := pipeNode.Pipes[1].(core.NodeListNode)
//	assert.True(t, ok)
//	assert.GreaterOrEqual(t, len(innerNodeListNode.Nodes), 5)
//
//	innerInnerNodeListNode, ok := innerNodeListNode.Nodes[4].(*core.NodeListNode)
//	assert.True(t, ok)
//	assert.Equal(t, 1, len(innerInnerNodeListNode.Nodes))
//
//	innerPipeNode, ok := innerInnerNodeListNode.Nodes[0].(*PipeNode)
//	assert.True(t, ok)
//
//	assert.Equal(t, `SELECT 1 as id, "test" as name |> WHERE id = 1`, transforms.ConcatTokenNodes(innerPipeNode))
//}
