package pipe_syntax

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/QuesmaOrg/quesma/platform/util"

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
		var minimumUnixTime, maximumUnixTime int64
		minimumUnixTime = 0
		maximumUnixTime = 0

		for i := 0; i < len(pipeNode.Pipes); i++ {
			pipeNodeList, ok := pipeNode.Pipes[i].(core.NodeListNode)
			if !ok {
				continue
			}
			if len(pipeNodeList.Nodes) < minimumExpandNodes {
				continue
			}

			if newPipes, handled := handleMacroOperator(pipeNodeList, &minimumUnixTime, &maximumUnixTime); handled {
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

func handleMacroOperator(pipeNodeList core.NodeListNode, minimumUnixTime *int64, maximumUnixTime *int64) ([]core.NodeListNode, bool) {
	// Support both "CALL" and "EXTEND" macros.
	tokenNode, ok := pipeNodeList.Nodes[2].(core.TokenNode)
	if !ok {
		return []core.NodeListNode{pipeNodeList}, false
	}
	operator := tokenNode.ValueUpper()
	if operator == "CALL" {
		// Determine the macro type from the 5th token.
		if len(pipeNodeList.Nodes) < minimumCallNodes {
			return []core.NodeListNode{pipeNodeList}, false
		}
		macroToken, ok := pipeNodeList.Nodes[macroTokenIdx].(core.TokenNode)
		if !ok {
			return []core.NodeListNode{pipeNodeList}, false
		}
		switch macroToken.ValueUpper() {
		case "TIME_BUCKET":
			return expandCallTimebucket(pipeNodeList), true
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
		switch macroToken.ValueUpper() {
		case "ENRICH_IP":
			return expandExtendEnrichIP(pipeNodeList), true
		case "ENRICH_IP_BOTS":
			return expandExtendEnrichIPBots(pipeNodeList), true
		case "ENRICH_LOG_CATEGORY":
			return expandExtendLogCategory(pipeNodeList), true
		case "PARSE_PATTERN":
			return expandExtendParsePattern(pipeNodeList), true
		default:
			return []core.NodeListNode{pipeNodeList}, false
		}
	} else if operator == "AGGREGATE" {
		for i := 0; i < len(pipeNodeList.Nodes)-1; i++ {
			if token, ok := pipeNodeList.Nodes[i].(core.TokenNode); ok && token.ValueUpper() == "TIME_BUCKET" {
				if innerList, ok := pipeNodeList.Nodes[i+1].(*core.NodeListNode); ok {
					innerContents := tokensToString(innerList.Nodes[1 : len(innerList.Nodes)-1])
					replacementToken := core.NewTokenNode(" time_bucket_" + innerContents)

					// Build a new pipe using buildTimebucketPipe with:
					// - timestampTokens as innerContents,
					// - intervalTokens hardcoded as "1 DAY",
					// - and nameTokens as replacementToken.
					timestampTokens := []core.Node{core.NewTokenNode(innerContents)}
					intervalTokens := []core.Node{core.NewTokenNode(" 1 DAY ")}
					if *maximumUnixTime != 0 && *minimumUnixTime != 0 {
						if (*maximumUnixTime-*minimumUnixTime)/(60*5) <= 26 {
							intervalTokens = []core.Node{core.NewTokenNode(" 5 MINUTE ")}
						} else if (*maximumUnixTime-*minimumUnixTime)/(60*60) <= 26 {
							intervalTokens = []core.Node{core.NewTokenNode(" 1 HOUR ")}
						} else if (*maximumUnixTime-*minimumUnixTime)/(60*60*2) <= 26 {
							intervalTokens = []core.Node{core.NewTokenNode(" 2 HOUR ")}
						} else if (*maximumUnixTime-*minimumUnixTime)/(60*60*4) <= 26 {
							intervalTokens = []core.Node{core.NewTokenNode(" 4 HOUR ")}
						} else if (*maximumUnixTime-*minimumUnixTime)/(60*60*8) <= 26 {
							intervalTokens = []core.Node{core.NewTokenNode(" 8 HOUR ")}
						} else if (*maximumUnixTime-*minimumUnixTime)/(60*60*24) <= 26 {
							intervalTokens = []core.Node{core.NewTokenNode(" 1 DAY ")}
						} else if (*maximumUnixTime-*minimumUnixTime)/(60*60*24*2) <= 26 {
							intervalTokens = []core.Node{core.NewTokenNode(" 2 DAY ")}
						} else if (*maximumUnixTime-*minimumUnixTime)/(60*60*24*4) <= 48 {
							intervalTokens = []core.Node{core.NewTokenNode(" 4 DAY ")}
						} else {
							intervalTokens = []core.Node{core.NewTokenNode(" 1 WEEK ")}
						}
					}
					nameTokens := []core.Node{replacementToken}
					newPipeNodes := buildTimebucketPipe(timestampTokens, intervalTokens, nameTokens)
					newPipe := core.NodeListNode{Nodes: newPipeNodes}

					// Replace the TIME_BUCKET and its following node with the new pipe and the replacement token.
					pipeNodeList.Nodes = append(slices.Clone(pipeNodeList.Nodes[:i]), append([]core.Node{replacementToken}, pipeNodeList.Nodes[i+2:]...)...)
					return []core.NodeListNode{newPipe, pipeNodeList}, true
				}
			}
		}
	} else if operator == "WHERE" {
		for i := 0; i < len(pipeNodeList.Nodes)-1; i++ {
			if token, ok := pipeNodeList.Nodes[i].(core.TokenNode); ok && token.ValueUpper() == "FROM_UNIXTIME" {
				if innerList, ok := pipeNodeList.Nodes[i+1].(*core.NodeListNode); ok && len(innerList.Nodes) == 3 {
					if openParen, ok := innerList.Nodes[0].(core.TokenNode); ok && openParen.Token.RawValue == "(" {
						if numberToken, ok := innerList.Nodes[1].(core.TokenNode); ok {
							if closeParen, ok := innerList.Nodes[2].(core.TokenNode); ok && closeParen.Token.RawValue == ")" {
								if parsed, err := strconv.ParseInt(strings.TrimSpace(numberToken.Token.RawValue), 10, 64); err == nil {
									if *minimumUnixTime != 0 {
										*minimumUnixTime = min(*minimumUnixTime, parsed)
									} else {
										*minimumUnixTime = parsed
									}
									if *maximumUnixTime != 0 {
										*maximumUnixTime = max(*maximumUnixTime, parsed)
									} else {
										*maximumUnixTime = parsed
									}
								}
							}
						}
					}
				}
			}
		}
	} else {
		applied := false
		for i := len(pipeNodeList.Nodes) - 2; i >= 0; i-- {
			if tokenNode, ok := pipeNodeList.Nodes[i].(core.TokenNode); ok && tokenNode.ValueUpper() == "TIME_BUCKET" {
				if innerList, ok := pipeNodeList.Nodes[i+1].(*core.NodeListNode); ok && len(innerList.Nodes) >= 2 {
					innerContents := tokensToString(innerList.Nodes[1 : len(innerList.Nodes)-1])
					replacementToken := core.NewTokenNode(" time_bucket_" + innerContents)
					pipeNodeList.Nodes = append(slices.Clone(pipeNodeList.Nodes[:i]), append([]core.Node{replacementToken}, pipeNodeList.Nodes[i+2:]...)...)
					applied = true
				}
			}
		}
		return []core.NodeListNode{pipeNodeList}, applied
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
			core.Add(&timestampTokens, pipeNodeList.Nodes[j])
		case 1:
			core.Add(&intervalTokens, pipeNodeList.Nodes[j])
		case 2:
			core.Add(&nameTokens, pipeNodeList.Nodes[j])
		}
	}

	pipe := buildTimebucketPipe(timestampTokens, intervalTokens, nameTokens)
	return []core.NodeListNode{{Nodes: pipe}}
}

// buildTimebucketPipe extracts the pipe-building logic for the TIME_BUCKET macro.
func buildTimebucketPipe(timestampTokens, intervalTokens, nameTokens core.Pipe) []core.Node {
	pipe := core.NewPipe(
		core.PipeToken(),
		core.Space(),
		// "EXTEND"
		core.Extend(),
		core.Space(),
		// First argument: formatDateTime(toStartOfInterval(timestamp, INTERVAL <interval>), '%m-%d %H:00')
		core.FormatDateTime(),
		core.LeftBracket(),
		core.ToStartOfInterval(),
		core.LeftBracket(),
	)
	core.Add(&pipe, timestampTokens...)
	core.Add(&pipe, core.Comma())
	core.Add(&pipe, core.Interval())
	core.Add(&pipe, intervalTokens...)
	// Close toStartOfInterval call.
	core.Add(&pipe, core.RightBracket())
	// End first argument list for formatDateTime: add comma and the format string.
	core.Add(&pipe, core.Comma())
	core.Add(&pipe, core.NewTokenNodeSingleQuote("%m-%d %H:%i"))
	// Close formatDateTime call.
	core.Add(&pipe, core.RightBracket())
	// Add "AS" and the alias tokens.
	core.Add(&pipe, core.Space())
	core.Add(&pipe, core.As())
	core.Add(&pipe, nameTokens...)

	return pipe
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

func expandExtendEnrichIPBots(pipeNodeList core.NodeListNode) []core.NodeListNode {
	// Expected form: |> EXTEND ENRICH_IP_BOTS(<ip column tokens>) AS <alias token>
	var ipTokens, aliasTokens []core.Node
	// Extract the ip column tokens from a nested node at index 5.
	if nested, ok := pipeNodeList.Nodes[5].(*core.NodeListNode); ok {
		if len(nested.Nodes) >= 3 {
			ipTokens = nested.Nodes[1 : len(nested.Nodes)-1]
		} else {
			ipTokens = nested.Nodes
		}
	}

	// Continue parsing after the nested ip column node to extract the alias.
	i := 6
	for ; i < len(pipeNodeList.Nodes); i++ {
		if token, ok := pipeNodeList.Nodes[i].(core.TokenNode); ok && strings.ToUpper(token.Token.RawValue) == "AS" {
			i++ // Skip "AS"
			// Optionally skip a whitespace token.
			if i < len(pipeNodeList.Nodes) {
				if ws, ok := pipeNodeList.Nodes[i].(core.TokenNode); ok && strings.TrimSpace(ws.Token.RawValue) == "" {
					i++
				}
			}
			break
		}
	}
	if i < len(pipeNodeList.Nodes) {
		aliasTokens = append(aliasTokens, pipeNodeList.Nodes[i])
	}

	// Build a new pipe representing:
	// |> EXTEND coalesce(enriched_ip.hostname ILIKE '%amazonaws%' OR enriched_ip.hostname ILIKE '%server%' OR enriched_ip.hostname ILIKE '%cloud%', false) AS <alias>
	var newNodes []core.Node
	newNodes = append(newNodes, core.PipeToken())
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.Extend())
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNode("coalesce"))
	newNodes = append(newNodes, core.LeftBracket())
	newNodes = append(newNodes, core.NewTokenNode(tokensToString(ipTokens)))
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNode("ILIKE"))
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNodeSingleQuote("%amazonaws%"))
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNode("OR"))
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNode(tokensToString(ipTokens)))
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNode("ILIKE"))
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNodeSingleQuote("%server%"))
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNode("OR"))
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNode(tokensToString(ipTokens)))
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNode("ILIKE"))
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNodeSingleQuote("%cloud%"))
	newNodes = append(newNodes, core.Comma())
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.NewTokenNode("false"))
	newNodes = append(newNodes, core.RightBracket())
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, core.As())
	newNodes = append(newNodes, core.Space())
	newNodes = append(newNodes, aliasTokens...)

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
		if token, ok := pipeNodeList.Nodes[i].(core.TokenNode); ok && token.ValueUpper() == "AS" {
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
	core.Add(&firstPipe, core.NewLine())

	return []core.NodeListNode{{Nodes: firstPipe}, {Nodes: secondPipe}}
}

func expandExtendLogCategory(pipeNodeList core.NodeListNode) []core.NodeListNode {
	// Expected form: |> EXTEND ENRICH_LOG_CATEGORY(<log_line>) AS <alias tokens>
	var logLineTokens []core.Node
	var aliasTokens []core.Node
	phase := 0
	for j := minimumCallNodes; j < len(pipeNodeList.Nodes); j++ {
		if token, ok := pipeNodeList.Nodes[j].(core.TokenNode); ok {
			if token.ValueUpper() == "AS" {
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

	regexClauses := []struct {
		regex    string
		thenText string
	}{
		{"Failed password for .* from .* port .* ssh2", "Failed password"},
		{"pam_unix\\(sshd:auth\\): authentication failure; logname= uid=.* euid=.* tty=ssh ruser= rhost=.*", "Auth failure"},
		{"reverse mapping checking getaddrinfo for .* \\[.*\\] failed - POSSIBLE BREAK-IN ATTEMPT!", "Break-in attempt"},
		{"pam_unix\\(sshd:auth\\): check pass; user unknown", "Unknown user"},
		{"Received disconnect from .*: .*: Bye Bye \\[preauth\\]", "Disconnect bye"},
		{"input_userauth_request: invalid user .* \\[preauth\\]", "Invalid user"},
		{"Accepted password for .* from .* port .* ssh2", "Accepted password"},
	}

	// Less common ones, commented out for now to avoid noise.
	//

	//{"Connection closed by .* \\[preauth\\]", "E2"},
	//{"Did not receive identification string from .*", "E3"},
	//{"Disconnecting: Too many authentication failures for admin \\[preauth\\]", "E4"},
	//{"Disconnecting: Too many authentication failures for root \\[preauth\\]", "E5"},
	//{"error: Received disconnect from .*: .*: com.jcraft.jsch.JSchException: Auth fail \\[preauth\\]", "E6"},
	//{"error: Received disconnect from .*: .*: No more user authentication methods available\\. \\[preauth\\]", "E7"},
	//{"Failed none for invalid user .* from .* port .* ssh2", "E8"},
	//
	//{"Failed password for invalid user .* from .* port .* ssh2", "E10"},
	//{"fatal: Write failed: Connection reset by peer \\[preauth\\]", "E11"},
	//
	//{"Invalid user .* from .*", "E13"},
	//{"message repeated .* times: \\[ Failed password for root from .* port .*\\]", "E14"},
	//{"PAM .* more authentication failure; logname= uid=.* euid=.* tty=ssh ruser= rhost=.*", "E15"},
	//{"PAM .* more authentication failures; logname= uid=.* euid=.* tty=ssh ruser= rhost=.*", "E16"},
	//{"PAM .* more authentication failures; logname= uid=.* euid=.* tty=ssh ruser= rhost=.*  user=root", "E17"},
	//{"PAM service\\(sshd\\) ignoring max retries; .* > .*", "E18"},
	//
	//{"pam_unix\\(sshd:auth\\): authentication failure; logname= uid=.* euid=.* tty=ssh ruser= rhost=.* user=.*", "E20"},
	//
	//{"pam_unix\\(sshd:session\\): session closed for user .*", "E22"},
	//{"pam_unix\\(sshd:session\\): session opened for user .* by \\(uid=.*\\)", "E23"},
	//
	//{"Received disconnect from .*: .*: Closed due to user request\\. \\[preauth\\]", "E25"},
	//{"Received disconnect from .*: .*: disconnected by user", "E26"},
	//}

	pipe := core.NewPipe(
		core.PipeToken(),
		core.Space(),
		core.Extend(),
		core.Space(),
		core.Case(),
	)

	for i, clause := range regexClauses {
		if i == 0 {
			core.Add(&pipe,
				core.Space(),
				core.When(),
				core.Space(),
			)
		} else {
			core.Add(&pipe, spaceWhenSpace()...)
		}
		core.Add(&pipe, logLineTokens...)
		core.Add(&pipe,
			core.Space(),
			core.Regexp(),
			core.Space(),
			core.NewTokenNodeSingleQuote(clause.regex),
			core.Space(),
			core.Then(),
			core.Space(),
			core.NewTokenNodeSingleQuote(clause.thenText),
		)
	}

	core.Add(&pipe,
		core.Space(),
		core.Else(),
		core.Space(),
		core.NewTokenNodeSingleQuote("Others"),
	)
	core.Add(&pipe,
		core.Space(),
		core.NewTokenNode("END"),
		core.Space(),
		core.As(),
		core.Space(),
	)
	core.Add(&pipe, aliasTokens...)

	return []core.NodeListNode{{Nodes: pipe}}
}

func buildFirstExtendPipe(msgTokens []core.Node, finalRegexLiteral, extractedAlias string) []core.Node {
	firstPipe := core.NewPipe(
		core.PipeToken(),
		core.Space(),
		core.Extend(),
		core.Space(),
		core.NewTokenNode("extractGroups"),
		core.LeftBracket(),
	)
	core.Add(&firstPipe, msgTokens...)
	core.Add(&firstPipe,
		core.Comma(),
		core.Space(),
		core.NewTokenNode(finalRegexLiteral),
		core.RightBracket(),
		core.Space(),
		core.As(),
		core.Space(),
		core.NewTokenNode(extractedAlias),
	)

	return firstPipe
}

func buildSecondExtendPipe(aliasParts []string, extractedAlias string) []core.Node {
	secondPipe := core.NewPipe(
		core.PipeToken(),
		core.Space(),
		core.Extend(),
		core.Space(),
	)
	for i, alias := range aliasParts {
		if i > 0 {
			core.Add(&secondPipe,
				core.Comma(),
				core.Space(),
			)
		}
		// Build tokens for: extracted_<msg>[<i+1>] AS <alias>
		core.Add(&secondPipe,
			core.NewTokenNode(extractedAlias),
			core.NewTokenNode("["),
			core.NewTokenNode(strconv.Itoa(i+1)),
			core.NewTokenNode("]"),
			core.Space(),
			core.As(),
			core.Space(),
			core.NewTokenNode(alias),
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
