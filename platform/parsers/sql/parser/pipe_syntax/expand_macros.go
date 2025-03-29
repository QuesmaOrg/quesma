package pipe_syntax

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/util"
	"regexp"
	"slices"
	"strconv"
	"strings"

	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
)

const (
	expandStartNode    = 5
	minimumExpandNodes = expandStartNode
	minimumCallNodes   = expandStartNode
	minimumExtendNodes = 7
	macroTokenIdx      = 4
)

func ExpandMacros(node core.Node) {
	TransformPipeNodes(node, func(pipeNode *PipeNode) core.Node {
		for i := 0; i < len(pipeNode.Pipes); i++ {
			pipeNodeList, ok := pipeNode.Pipes[i].(core.NodeListNode)
			if !ok {
				continue
			}
			if len(pipeNodeList.Nodes) < minimumExpandNodes {
				continue
			}

			if newPipes, handled := handleMacroOperator(pipeNodeList); handled {
				var convertedNewPipes []core.Node
				for _, np := range newPipes {
					convertedNewPipes = append(convertedNewPipes, np)
				}
				newPipes := slices.Clone(pipeNode.Pipes)
				pipeNode.Pipes = append(append(newPipes[:i], convertedNewPipes...), pipeNode.Pipes[i+1:]...)
			}
		}
		return pipeNode
	})
}

func handleMacroOperator(pipeNodeList core.NodeListNode) ([]core.NodeListNode, bool) {
	// Support both "CALL" and "EXTEND" macros.
	tokenNode, ok := pipeNodeList.Nodes[2].(core.TokenNode)
	if !ok {
		return []core.NodeListNode{pipeNodeList}, false
	}
	operator := strings.ToUpper(tokenNode.Token.RawValue)
	if operator == "CALL" {
		// Determine the macro type from the 5th token.
		if len(pipeNodeList.Nodes) < minimumCallNodes {
			return []core.NodeListNode{pipeNodeList}, false
		}
		macroToken, ok := pipeNodeList.Nodes[macroTokenIdx].(core.TokenNode)
		if !ok {
			return []core.NodeListNode{pipeNodeList}, false
		}
		macroType := strings.ToUpper(macroToken.Token.RawValue)
		switch macroType {
		case "TIMEBUCKET":
			return expandCallTimebucket(pipeNodeList), true
		case "LOGCATEGORY":
			return expandCallLogCategory(pipeNodeList), true
		default:
			// Macro not recognized; do nothing.
			return []core.NodeListNode{pipeNodeList}, false
		}
	} else if operator == "EXTEND" {
		if len(pipeNodeList.Nodes) < minimumExtendNodes {
			return []core.NodeListNode{pipeNodeList}, false
		}
		macroToken, ok := pipeNodeList.Nodes[macroTokenIdx].(core.TokenNode)
		if !ok {
			return []core.NodeListNode{pipeNodeList}, false
		}
		macroName := strings.ToUpper(macroToken.Token.RawValue)
		switch macroName {
		case "ENRICH_IP":
			return expandExtendEnrichIP(pipeNodeList), true
		case "PARSE_PATTERN":
			return expandExtendParsePattern(pipeNodeList), true
		default:
			return []core.NodeListNode{pipeNodeList}, false
		}
	}
	// Operator not recognized.
	return []core.NodeListNode{pipeNodeList}, false
}

func expandCallTimebucket(pipeNodeList core.NodeListNode) []core.NodeListNode {
	// Expected form: |> CALL TIMEBUCKET <timestamp> BY <interval tokens> AS <alias tokens>
	var timestampTokens, intervalTokens, nameTokens core.Pipe
	phase := 0
	for j := minimumCallNodes; j < len(pipeNodeList.Nodes); j++ {
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
			core.Add(timestampTokens, pipeNodeList.Nodes[j])
		case 1:
			core.Add(intervalTokens, pipeNodeList.Nodes[j])
		case 2:
			core.Add(nameTokens, pipeNodeList.Nodes[j])
		}
	}

	// Build a new pipe representing:
	// |> EXTEND concat(
	//       formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>), '%Y-%m-%d %H:00'),
	//       ' - ',
	//       formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>) + INTERVAL <interval>, '%Y-%m-%d %H:00')
	//    ) AS <alias>
	pipe := core.NewPipe(
		core.PipeToken(),
		core.Space(),
		// "EXTEND"
		core.Extend(),
		core.Space(),
		// "concat"
		core.NewTokenNode("concat"),
		// "("
		core.LeftBracket(),
		// First argument: formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>), '%Y-%m-%d %H:00')
		core.NewTokenNode("formatDateTime"),
		core.LeftBracket(),
		core.NewTokenNode("toStartOfInterval"),
		core.LeftBracket(),
	)
	core.Add(pipe, timestampTokens...)
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "INTERVAL"}})
	core.Add(pipe, intervalTokens...)
	// Close toStartOfInterval call.
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
	// End first argument list for formatDateTime: add comma and the format string.
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "'%Y-%m-%d %H:00'"}})
	// Close formatDateTime call.
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
	// Separator for concat arguments.
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
	// Second argument: the literal separator ' - '
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "' - '"}})
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
	// Second argument: formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>) + INTERVAL <interval>, '%Y-%m-%d %H:00')
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "formatDateTime"}})
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "("}})
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "toStartOfInterval"}})
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "("}})
	core.Add(pipe, timestampTokens...)
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "INTERVAL"}})
	core.Add(pipe, intervalTokens...)
	// Close the first toStartOfInterval call.
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
	// Add the plus operator and the second "INTERVAL" for addition.
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "+"}})
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "INTERVAL"}})
	core.Add(pipe, intervalTokens...)
	// Add comma and the format string.
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "'%Y-%m-%d %H:00'"}})
	// Close the second formatDateTime call.
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
	// Close the concat call.
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
	// Add "AS" and the alias tokens.
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
	core.Add(pipe, core.TokenNode{Token: lexer_core.Token{RawValue: "AS"}})
	core.Add(pipe, nameTokens...)

	return []core.NodeListNode{{Nodes: pipe}}
}

func expandExtendEnrichIP(pipeNodeList core.NodeListNode) []core.NodeListNode {
	// Expected form: |> EXTEND ENRICH_IP(<ip column tokens>) AS <alias tokens>
	var ipTokens, aliasTokens []core.Node
	// The expected form is: |> EXTEND ENRICH_IP(<ip_column_tokens>) AS <alias_tokens>
	// Here, <ip_column_tokens> is a single nested core.NodeListNode at index 5 that contains at least three tokens: an opening "(", the actual ip column tokens, and a closing ")".
	if nested, ok := pipeNodeList.Nodes[5].(*core.NodeListNode); ok {
		if len(nested.Nodes) >= 3 {
			// Extract the ip column tokens by removing the surrounding parentheses.
			ipTokens = nested.Nodes[1 : len(nested.Nodes)-1]
		} else {
			ipTokens = nested.Nodes
		}
	}

	// Continue parsing after the nested ip column node.
	i := 6
	// Skip tokens until the "AS" keyword is encountered.
	for ; i < len(pipeNodeList.Nodes); i++ {
		if token, ok := pipeNodeList.Nodes[i].(core.TokenNode); ok && strings.ToUpper(token.Token.RawValue) == "AS" {
			i++ // Skip the "AS" token.
			i++ // Skip the whitespace token.
			break
		}
	}
	// The remaining 1 token are the alias tokens.
	for ; i < len(pipeNodeList.Nodes); i++ {
		aliasTokens = append(aliasTokens, pipeNodeList.Nodes[i])
		break
	}

	// Convert ipTokens and aliasTokens to strings.
	ipExprStr := tokensToString(ipTokens)
	aliasStr := tokensToString(aliasTokens)

	var selectColumns []core.Node
	columns := []string{
		"allocated_at",
		"asn",
		"asn_country",
		"city",
		"country_long",
		"country_short",
		"hostname",
		"ip",
		"isp",
		"latitude",
		"longitude",
		"region",
		"registry",
		"timezone",
		"zipcode",
	}
	for j, col := range columns {
		if j > 0 {
			selectColumns = append(selectColumns,
				core.Comma(),
				core.Space(),
			)
		}
		selectColumns = append(selectColumns,
			core.NewTokenNode(col),
			core.Space(),
			core.As(),
			core.Space(),
			core.NewTokenNode(fmt.Sprintf(`"%s.%s"`, aliasStr, col)),
		)
	}

	newNodes := []core.Node{
		core.PipeToken(),
		core.Space(),
		core.LeftJoin(),
		core.Space(),
		core.LeftBracket(),
		core.Select(),
		core.Space(),
	}
	newNodes = append(newNodes, selectColumns...)
	newNodes = append(newNodes,
		core.Space(),
		core.From(),
		core.Space(),
		core.NewTokenNode("ip_data"),
		core.RightBracket(),
		core.Space(),
		core.On(),
		core.Space(),
		core.NewTokenNode(aliasStr+".ip"),
		core.Space(),
		core.Equals(),
		core.Space(),
		core.NewTokenNode(ipExprStr),
	)

	return []core.NodeListNode{{Nodes: newNodes}}
}
func expandExtendParsePattern(pipeNodeList core.NodeListNode) []core.NodeListNode {
	// Expected form:
	//   |> EXTEND PARSE_PATTERN(<msg>, <pattern>) AS <alias1>, <alias2>, <alias3>, ...
	//
	// Transformation:
	//   |> EXTEND extractGroups(<msg>, '<regex>') AS extracted_<msg>
	//   |> EXTEND extracted_<msg>[1] AS <alias1>, extracted_<msg>[2] AS <alias2>, extracted_<msg>[3] AS <alias3>, ...
	//
	// The <regex> is built by replacing each "%" in <pattern> with "(.*)"
	// and escaping all other characters using regexp.QuoteMeta.

	// Extract the parameters from the nested node at index 5.
	var msgTokens, patternTokens []core.Node
	nested, ok := pipeNodeList.Nodes[5].(*core.NodeListNode)
	if !ok {
		return []core.NodeListNode{pipeNodeList}
	}
	// Remove surrounding parentheses.
	if len(nested.Nodes) >= 2 {
		params := nested.Nodes[1 : len(nested.Nodes)-1]
		// Split tokens by comma.
		var parts [][]core.Node
		current := make([]core.Node, 0)
		for _, token := range params {
			if tk, ok := token.(core.TokenNode); ok && strings.TrimSpace(tk.Token.RawValue) == "," {
				parts = append(parts, current)
				current = []core.Node{}
			} else {
				current = append(current, token)
			}
		}
		if len(current) > 0 {
			parts = append(parts, current)
		}
		if len(parts) != 2 {
			// Expected exactly 2 parameters.
			return []core.NodeListNode{pipeNodeList}
		}
		msgTokens = parts[0]
		patternTokens = parts[1]
	} else {
		return []core.NodeListNode{pipeNodeList}
	}

	// Extract alias tokens after the nested parameters.
	var aliasTokens []core.Node
	for i := 6; i < len(pipeNodeList.Nodes); i++ {
		if token, ok := pipeNodeList.Nodes[i].(core.TokenNode); ok && strings.ToUpper(token.Token.RawValue) == "AS" {
			// Collect all tokens after the "AS".
			i++
			for ; i < len(pipeNodeList.Nodes); i++ {
				aliasTokens = append(aliasTokens, pipeNodeList.Nodes[i])
			}
			break
		}
	}

	// Convert pattern tokens to string.
	rawPattern := tokensToString(patternTokens)
	rawPattern = strings.TrimSpace(rawPattern)
	// Remove surrounding quotes if present.
	rawPattern = util.UnquoteIfQuoted(rawPattern)

	// Build the regex by splitting on '%' and escaping each part.
	splitParts := strings.Split(rawPattern, "%")
	for i, part := range splitParts {
		splitParts[i] = regexp.QuoteMeta(part)
	}
	finalRegex := strings.Join(splitParts, "(.*)")
	// Wrap the final regex in single quotes.
	finalRegexLiteral := util.SingleQuote(finalRegex)

	// Determine the extracted alias based on the <msg> parameter.
	msgStr := strings.TrimSpace(tokensToString(msgTokens))
	extractedAlias := "extracted_" + msgStr

	// Build the first pipe:
	//   |> EXTEND extractGroups(<msg>, <finalRegexLiteral>) AS extracted_<msg>
	firstPipe := buildFirstExtendPipe(msgTokens, finalRegexLiteral, extractedAlias)

	// Process alias tokens into individual alias names.
	aliasStr := tokensToString(aliasTokens)
	aliasParts := strings.Split(aliasStr, ",")
	for i := range aliasParts {
		aliasParts[i] = strings.TrimSpace(aliasParts[i])
	}

	// Build the second pipe:
	//   |> EXTEND extracted_<msg>[1] AS <alias1>, extracted_<msg>[2] AS <alias2>, ...
	secondPipe := buildSecondExtendPipe(aliasParts, extractedAlias)

	// Combine both pipe commands into a single node list, separating them with a newline.
	firstPipe = append(firstPipe, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})

	return []core.NodeListNode{{Nodes: firstPipe}, {Nodes: secondPipe}}
}

func expandCallLogCategory(pipeNodeList core.NodeListNode) []core.NodeListNode {
	// Expected form: |> CALL LOGCATEGORY <log_line> AS <alias tokens>
	var logLineTokens []core.Node
	var aliasTokens []core.Node
	phase := 0
	for j := minimumCallNodes; j < len(pipeNodeList.Nodes); j++ {
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

	spaceWhenSpace := func() []core.Node {
		return []core.Node{
			core.Space(),
			core.When(),
			core.Space(),
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
	pipe := core.NewPipe(
		core.PipeToken(),
		core.Space(),
		core.Extend(),
		core.Space(),
		core.Case(),
	)
	// Clause 1
	core.Add(pipe,
		core.Space(),
		core.When(),
		core.Space(),
	)
	core.Add(pipe, logLineTokens...)
	core.Add(pipe,
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
	core.Add(pipe, spaceWhenSpace()...)
	core.Add(pipe, logLineTokens...)
	core.Add(pipe,
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
	core.Add(pipe, spaceWhenSpace()...)
	core.Add(pipe, logLineTokens...)
	core.Add(pipe,
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
	core.Add(pipe, spaceWhenSpace()...)
	core.Add(pipe, logLineTokens...)
	core.Add(pipe,
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
	core.Add(pipe, spaceWhenSpace()...)
	core.Add(pipe, logLineTokens...)
	core.Add(pipe,
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
	core.Add(pipe,
		core.Space(),
		core.Else(),
		core.Space(),
		core.NewTokenNode("'Unknown'"),
	)
	// End clause: END AS <alias tokens>
	core.Add(pipe,
		core.Space(),
		core.NewTokenNode("END"),
		core.Space(),
		core.As(),
		core.Space(),
	)
	core.Add(pipe, aliasTokens...)

	return []core.NodeListNode{{Nodes: pipe}}
}

func buildFirstExtendPipe(msgTokens []core.Node, finalRegexLiteral, extractedAlias string) []core.Node {
	firstPipe := []core.Node{
		core.TokenNode{Token: lexer_core.Token{RawValue: "|>"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "EXTEND"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "extractGroups"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "("}},
	}
	firstPipe = append(firstPipe, msgTokens...)
	firstPipe = append(firstPipe,
		core.TokenNode{Token: lexer_core.Token{RawValue: ","}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: finalRegexLiteral}},
		core.TokenNode{Token: lexer_core.Token{RawValue: ")"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "AS"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: extractedAlias}},
	)

	return firstPipe
}

func buildSecondExtendPipe(aliasParts []string, extractedAlias string) []core.Node {
	secondPipe := []core.Node{
		core.TokenNode{Token: lexer_core.Token{RawValue: "|>"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
		core.TokenNode{Token: lexer_core.Token{RawValue: "EXTEND"}},
		core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
	}
	for i, alias := range aliasParts {
		if i > 0 {
			secondPipe = append(secondPipe,
				core.TokenNode{Token: lexer_core.Token{RawValue: ","}},
				core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
			)
		}
		// Build tokens for: extracted_<msg>[<i+1>] AS <alias>
		secondPipe = append(secondPipe,
			core.TokenNode{Token: lexer_core.Token{RawValue: extractedAlias}},
			core.TokenNode{Token: lexer_core.Token{RawValue: "["}},
			core.TokenNode{Token: lexer_core.Token{RawValue: strconv.Itoa(i + 1)}},
			core.TokenNode{Token: lexer_core.Token{RawValue: "]"}},
			core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
			core.TokenNode{Token: lexer_core.Token{RawValue: "AS"}},
			core.TokenNode{Token: lexer_core.Token{RawValue: " "}},
			core.TokenNode{Token: lexer_core.Token{RawValue: alias}},
		)
	}

	return secondPipe
}

func tokensToString(tokens []core.Node) string {
	var sb strings.Builder
	for _, node := range tokens {
		if token, ok := node.(core.TokenNode); ok {
			sb.WriteString(token.Token.RawValue)
		}
	}
	return sb.String()
}
