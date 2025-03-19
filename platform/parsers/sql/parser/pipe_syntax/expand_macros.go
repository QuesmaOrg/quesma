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

			// Verify we have a "CALL" operator.
			tokenNode, ok := pipeNodeList.Nodes[2].(core.TokenNode)
			if !ok || strings.ToUpper(tokenNode.Token.RawValue) != "CALL" {
				continue
			}

			// Determine the macro type from the 5th token.
			macroToken, ok := pipeNodeList.Nodes[4].(core.TokenNode)
			if !ok {
				continue
			}
			macroType := strings.ToUpper(macroToken.Token.RawValue)

			if macroType == "TIMEBUCKET" {
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
				//       formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>), '%Y-%m-%d %H:00'),
				//       ' - ',
				//       formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>) + INTERVAL <interval>, '%Y-%m-%d %H:00')
				//    ) AS <alias>
				newNodes := []core.Node{
					core.TokenNode{Token: lexer_core.Token{RawValue: "|>"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					// "EXTEND"
					core.TokenNode{Token: lexer_core.Token{RawValue: "EXTEND"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					// "concat"
					core.TokenNode{Token: lexer_core.Token{RawValue: "concat"}},
					// "("
					core.TokenNode{Token: lexer_core.Token{RawValue: "("}},
					// First argument: formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>), '%Y-%m-%d %H:00')
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
				// Second argument: formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>) + INTERVAL <interval>, '%Y-%m-%d %H:00')
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
			} else if macroType == "LOGCATEGORY" {
				// Parse out the tokens following "CALL LOGCATEGORY":
				// Expected form: |> CALL LOGCATEGORY <log_line> AS <alias tokens>
				var logLineTokens []core.Node
				var aliasTokens []core.Node
				phase := 0
				for j := 5; j < len(pipeNodeList.Nodes); j++ {
					if token, ok := pipeNodeList.Nodes[j].(core.TokenNode); ok {
						if strings.ToUpper(token.Token.RawValue) == "AS" {
							phase = 1
							continue
						}
					}
					if phase == 0 {
						logLineTokens = append(logLineTokens, pipeNodeList.Nodes[j])
					} else {
						aliasTokens = append(aliasTokens, pipeNodeList.Nodes[j])
					}
				}

				// Build a new pipe representing:
				// |> extend CASE
				//     WHEN <log_line> REGEXP '\\{"code":200,"message":"success"\\}' THEN 'JSON API Response'
				//     WHEN <log_line> REGEXP '\\[\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}\\] \\[ info\\] \\[output:http:http\\.\\d+\\] .+?, HTTP status=200' THEN 'HTTP Output'
				//     WHEN <log_line> REGEXP 'action ''action-\\d+-builtin:omfile'' \\(module ''builtin:omfile''\\) message lost, could not be processed\\. Check for additional error messages before this one\\.' THEN 'Rsyslog Message Lost'
				//     WHEN <log_line> REGEXP '(no space left on device|write error - see https://www\\.rsyslog\\.com/solving-rsyslog-write-errors/)' THEN 'Disk Space Error'
				//     WHEN <log_line> REGEXP '(Failed password for|Invalid user|Disconnected from) .+? port \\d+' THEN 'SSH Authentication Error'
				//     ELSE 'Unknown'
				// END AS <alias tokens>
				newNodes := []core.Node{
					core.TokenNode{Token: lexer_core.Token{RawValue: "|>"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "extend"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "CASE"}},
				}
				// Clause 1
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "WHEN"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
				newNodes = append(newNodes, logLineTokens...)
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "REGEXP"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'\\\\{\"code\":200,\"message\":\"success\"\\\\}'"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "THEN"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'JSON API Response'"}},
				)
				// Clause 2
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "WHEN"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
				newNodes = append(newNodes, logLineTokens...)
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "REGEXP"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'\\\\[\\\\d{4}/\\\\d{2}/\\\\d{2} \\\\d{2}:\\\\d{2}:\\\\d{2}\\\\] \\\\[ info\\\\] \\\\[output:http:http\\\\.\\\\d+\\\\] .+?, HTTP status=200'"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "THEN"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'HTTP Output'"}},
				)
				// Clause 3
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "WHEN"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
				newNodes = append(newNodes, logLineTokens...)
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "REGEXP"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'action ''action-\\\\d+-builtin:omfile'' \\(module ''builtin:omfile''\\) message lost, could not be processed\\. Check for additional error messages before this one\\.'"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "THEN"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'Rsyslog Message Lost'"}},
				)
				// Clause 4
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "WHEN"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
				newNodes = append(newNodes, logLineTokens...)
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "REGEXP"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'(no space left on device|write error - see https://www\\\\.rsyslog\\\\.com/solving-rsyslog-write-errors/)'"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "THEN"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'Disk Space Error'"}},
				)
				// Clause 5
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "WHEN"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
				newNodes = append(newNodes, logLineTokens...)
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "REGEXP"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'(Failed password for|Invalid user|Disconnected from) .+? port \\d+'"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "THEN"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'SSH Authentication Error'"}},
				)
				// ELSE clause
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "ELSE"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "'Unknown'"}},
				)
				// End clause: END AS <alias tokens>
				newNodes = append(newNodes,
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "END"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
					core.TokenNode{Token: lexer_core.Token{RawValue: "AS"}},
					core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
				newNodes = append(newNodes, aliasTokens...)

				// Replace the old macro pipe with the expanded one.
				pipeNode.Pipes[i] = core.NodeListNode{Nodes: newNodes}
			} else {
				// Macro not recognized; continue.
				continue
			}
		}
		return pipeNode
	})
}
