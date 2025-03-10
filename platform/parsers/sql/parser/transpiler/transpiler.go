package transpiler

import (
	"fmt"
	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/transforms"
	"slices"
	"strings"
)

type TranspileState struct {
	selectNode   core.Node
	from         core.Node
	join         core.Node
	where        core.Node
	orderby      core.Node
	limit        core.Node
	groupby      core.Node
	lastPriority int
}

func Transpile(node core.Node) {
	transformPipeNodes(node, func(pipeNode *transforms.PipeNode) core.Node {
		state := TranspileState{}

		if pipeNodeList, ok := pipeNode.BeforePipe.(core.NodeListNode); ok {
			if strings.ToUpper(pipeNodeList.Nodes[0].(core.TokenNode).Token.RawValue) == "FROM" {
				state.from = core.NodeListNode{Nodes: pipeNodeList.Nodes[1:]}
			} else {
				state.from = pipeNodeList
			}
		} else {
			state.from = core.NodeListNode{Nodes: []core.Node{pipeNode.BeforePipe}}
		}

		for _, pipe := range pipeNode.Pipes {
			if pipeNodeList, ok := pipe.(core.NodeListNode); ok {
				name := strings.ToUpper(pipeNodeList.Nodes[2].(core.TokenNode).Token.RawValue)
				switch name {
				case "JOIN":
					if state.lastPriority >= 2 {
						state = TranspileState{from: renderState(state, true)}
					}
					state.join = pipeNodeList
					state.lastPriority = 2
				case "WHERE":
					if state.lastPriority >= 3 {
						state = TranspileState{from: renderState(state, true)}
					}
					state.where = pipeNodeList
					state.lastPriority = 3
				case "AGGREGATE":
					if state.lastPriority >= 4 {
						state = TranspileState{from: renderState(state, true)}
					}
					state.lastPriority = 4

					var groupby []core.Node
					var selectNodes []core.Node

					groupbyPart := false
					for _, node := range pipeNodeList.Nodes[3:] {
						if tokenNode, ok := node.(core.TokenNode); ok && strings.ToUpper(tokenNode.Token.RawValue) == "GROUP BY" {
							groupbyPart = true
							continue
						}

						if !groupbyPart {
							selectNodes = append(selectNodes, node)
						} else {
							groupby = append(groupby, node)
						}
					}

					state.groupby = core.NodeListNode{Nodes: groupby}
					if selectNode, ok := state.selectNode.(core.NodeListNode); ok {
						selectNode.Nodes = append(selectNode.Nodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
						selectNode.Nodes = append(selectNode.Nodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
						selectNode.Nodes = append(selectNode.Nodes, selectNodes...)
						selectNode.Nodes = append(selectNode.Nodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
						selectNode.Nodes = append(selectNode.Nodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
						selectNode.Nodes = append(selectNode.Nodes, groupby...)
						state.selectNode = selectNode
					} else {
						var allNodes []core.Node
						allNodes = slices.Clone(selectNodes)
						allNodes = append(allNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
						allNodes = append(allNodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
						allNodes = append(allNodes, groupby...)
						state.selectNode = core.NodeListNode{Nodes: allNodes}
					}
				case "ORDER BY":
					if state.lastPriority >= 9 {
						state = TranspileState{from: renderState(state, true)}
					}
					state.orderby = pipeNodeList
					state.lastPriority = 9
				case "SELECT":
					if state.lastPriority >= 7 {
						state = TranspileState{from: renderState(state, true)}
					}
					state.selectNode = core.NodeListNode{Nodes: pipeNodeList.Nodes[3:]}
					state.lastPriority = 7
				case "LIMIT":
					if state.lastPriority >= 10 {
						state = TranspileState{from: renderState(state, true)}
					}
					state.limit = pipeNodeList
					state.lastPriority = 10
				default:
					fmt.Println("Unknown pipe: ", name)
				}
			}
		}

		return renderState(state, false)
	})
}

func renderState(state TranspileState, parens bool) core.Node {
	var nodes []core.Node

	if parens {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "("}})
	}
	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "SELECT"}})
	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
	if state.selectNode == nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "*"}})
	} else {
		selectNode := state.selectNode.(core.NodeListNode)
		for _, node := range selectNode.Nodes {
			nodes = append(nodes, node)
		}
	}
	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "FROM"}})
	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
	nodes = append(nodes, state.from)
	if state.join != nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
		join := state.join.(core.NodeListNode)
		for i, node := range join.Nodes {
			if i > 1 {
				nodes = append(nodes, node)
			}
		}
	}
	if state.where != nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
		where := state.where.(core.NodeListNode)
		for i, node := range where.Nodes {
			if i > 1 {
				nodes = append(nodes, node)
			}
		}
	}
	if state.groupby != nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "GROUP BY"}})
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
		groupby := state.groupby.(core.NodeListNode)
		for _, node := range groupby.Nodes {
			nodes = append(nodes, node)
		}
	}
	if state.orderby != nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
		orderby := state.orderby.(core.NodeListNode)
		for i, node := range orderby.Nodes {
			if i > 1 {
				nodes = append(nodes, node)
			}
		}
	}
	if state.limit != nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
		limit := state.limit.(core.NodeListNode)
		for i, node := range limit.Nodes {
			if i > 1 {
				nodes = append(nodes, node)
			}
		}
	}
	if parens {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
	}
	return &core.NodeListNode{Nodes: nodes}
}

func transformPipeNodes(node core.Node, visitor func(pipeNode *transforms.PipeNode) core.Node) {
	for _, child := range node.Children() {
		transformPipeNodes(child, visitor)
	}
	if nodeListNode, ok := node.(*core.NodeListNode); ok {
		for i, child := range nodeListNode.Nodes {
			if pipeNode, ok := child.(*transforms.PipeNode); ok {
				nodeListNode.Nodes[i] = visitor(pipeNode)
			}
		}
	}
}
