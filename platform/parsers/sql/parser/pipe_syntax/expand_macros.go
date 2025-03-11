// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package pipe_syntax

import (
	"strings"

	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
)

func ExpandMacros(node core.Node) {
	TransformPipeNodes(node, func(pipeNode *PipeNode) core.Node {
		for i, pipe := range pipeNode.Pipes {
			pipeNodeList, ok := pipe.(core.NodeListNode)
			if !ok {
				continue
			}

			if len(pipeNodeList.Nodes) < 5 {
				continue
			}

			// Verify we have a "CALL" operator and a "TIMEBUCKET" macro.
			if tokenNode, ok := pipeNodeList.Nodes[2].(core.TokenNode); !ok || strings.ToUpper(tokenNode.Token.RawValue) != "CALL" {
				continue
			}
			if tokenNode, ok := pipeNodeList.Nodes[4].(core.TokenNode); !ok || strings.ToUpper(tokenNode.Token.RawValue) != "TIMEBUCKET" {
				continue
			}

			// Parse out the tokens following "CALL TIMEBUCKET":
			// Expected form: |> CALL TIMEBUCKET <timestamp> BY <interval tokens> AS <alias tokens>
			var timestampTokens, intervalTokens, nameTokens []core.Node
			phase := 0
			for j := 5; j < len(pipeNodeList.Nodes); j++ {
				if tokenNode, ok := pipeNodeList.Nodes[j].(core.TokenNode); ok {
					switch strings.ToUpper(tokenNode.Token.RawValue) {
					case "BY":
						phase = 1
						continue
					case "AS":
						phase = 2
						continue
					}
				}
				switch phase {
				case 0:
					timestampTokens = append(timestampTokens, pipeNodeList.Nodes[j])
				case 1:
					intervalTokens = append(intervalTokens, pipeNodeList.Nodes[j])
				case 2:
					nameTokens = append(nameTokens, pipeNodeList.Nodes[j])
				}
			}

			// Build a new pipe representing:
			// |> EXTEND concat(
			//       formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>), '%Y-%m-%d %H:%M'),
			//       ' - ',
			//       formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>) + INTERVAL <interval>, '%Y-%m-%d %H:%M')
			//    ) AS <alias>
			newNodes := []core.Node{
				core.TokenNode{Token: lexer_core.Token{RawValue: "|>"}},
				core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
				// "EXTEND"
				core.TokenNode{Token: lexer_core.Token{RawValue: "EXTEND"}},
				// "concat"
				core.TokenNode{Token: lexer_core.Token{RawValue: "concat"}},
				// "("
				core.TokenNode{Token: lexer_core.Token{RawValue: "("}},
				// First argument: formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>), '%Y-%m-%d %H:%M')
				core.TokenNode{Token: lexer_core.Token{RawValue: "formatDateTime"}},
				core.TokenNode{Token: lexer_core.Token{RawValue: "("}},
				core.TokenNode{Token: lexer_core.Token{RawValue: "toStartOfInterval"}},
				core.TokenNode{Token: lexer_core.Token{RawValue: "("}},
			}
			newNodes = append(newNodes, timestampTokens...)
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "INTERVAL"}})
			newNodes = append(newNodes, intervalTokens...)
			// Close toStartOfInterval call.
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
			// End first argument list for formatDateTime: add comma and the format string.
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "'%Y-%m-%d %H:00'"}})
			// Close formatDateTime call.
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
			// Separator for concat arguments.
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
			// Second argument: the literal separator ' - '
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "' - '"}})
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
			// Second argument: formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>) + INTERVAL <interval>, '%Y-%m-%d %H:%M')
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "formatDateTime"}})
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "("}})
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "toStartOfInterval"}})
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "("}})
			newNodes = append(newNodes, timestampTokens...)
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "INTERVAL"}})
			newNodes = append(newNodes, intervalTokens...)
			// Close the first toStartOfInterval call.
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
			// Add the plus operator and the second "INTERVAL" for addition.
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "+"}})
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "INTERVAL"}})
			newNodes = append(newNodes, intervalTokens...)
			// Add comma and the format string.
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "'%Y-%m-%d %H:00'"}})
			// Close the second formatDateTime call.
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
			// Close the concat call.
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
			// Add "AS" and the alias tokens.
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
			newNodes = append(newNodes, core.TokenNode{Token: lexer_core.Token{RawValue: "AS"}})
			newNodes = append(newNodes, nameTokens...)

			// Replace the old macro pipe with the expanded one.
			pipeNode.Pipes[i] = core.NodeListNode{Nodes: newNodes}
		}

		return pipeNode
	})
}
