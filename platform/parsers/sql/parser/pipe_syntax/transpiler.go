package pipe_syntax

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
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
	TransformPipeNodes(node, func(pipeNode *PipeNode) core.Node {
		state := TranspileState{}

		if pipeNodeList, ok := pipeNode.BeforePipe.(core.NodeListNode); ok {
			if pipeNodeList.Nodes[0].(core.TokenNode).ValueUpper() == "FROM" {
				state.from = core.NodeListNode{Nodes: pipeNodeList.Nodes[1:]}
			} else {
				state.from = pipeNodeList
			}
		} else {
			state.from = core.NodeListNode{Nodes: []core.Node{pipeNode.BeforePipe}}
		}

		for _, pipe := range pipeNode.Pipes {
			if pipeNodeList, ok := pipe.(core.NodeListNode); ok {
				name := pipeNodeList.Nodes[2].(core.TokenNode).ValueUpper()

				// JOIN, CROSS JOIN, LEFT OUTER JOIN etc.
				if strings.HasSuffix(name, "JOIN") {
					if state.lastPriority >= 2 {
						state = TranspileState{from: renderState(state, true)}
					}
					state.join = pipeNodeList
					state.lastPriority = 2

					continue
				}

				switch name {
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
						if tokenNode, ok := node.(core.TokenNode); ok && tokenNode.ValueUpper() == "GROUP BY" {
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
						selectNode.Nodes = append(selectNode.Nodes, core.Comma())
						selectNode.Nodes = append(selectNode.Nodes, core.Space())
						selectNode.Nodes = append(selectNode.Nodes, groupby...)
						selectNode.Nodes = append(selectNode.Nodes, core.Comma())
						selectNode.Nodes = append(selectNode.Nodes, core.Space())
						selectNode.Nodes = append(selectNode.Nodes, selectNodes...)
						state.selectNode = selectNode
					} else {
						var allNodes []core.Node
						allNodes = slices.Clone(groupby)
						allNodes = append(allNodes, core.Comma())
						allNodes = append(allNodes, core.Space())
						allNodes = append(allNodes, selectNodes...)
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
				case "EXTEND":
					if state.lastPriority >= 8 {
						state = TranspileState{from: renderState(state, true)}
					}
					if state.selectNode != nil {
						var allNodes []core.Node
						allNodes = slices.Clone(state.selectNode.(core.NodeListNode).Nodes)
						allNodes = append(allNodes, core.Space())
						allNodes = append(allNodes, core.Space())
						allNodes = append(allNodes, pipeNodeList.Nodes[3:]...)
						state.selectNode = core.NodeListNode{Nodes: allNodes}
					} else {
						var allNodes []core.Node
						allNodes = []core.Node{core.Star()}
						allNodes = append(allNodes, core.Comma())
						allNodes = append(allNodes, core.Space())
						allNodes = append(allNodes, pipeNodeList.Nodes[3:]...)
						state.selectNode = core.NodeListNode{Nodes: allNodes}
					}
					state.lastPriority = 8
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
		nodes = append(nodes, core.LeftBracket())
	}
	nodes = append(nodes, core.Select())
	nodes = append(nodes, core.Space())
	if state.selectNode == nil {
		nodes = append(nodes, core.Star())
	} else {
		selectNode := state.selectNode.(core.NodeListNode)
		for _, node := range selectNode.Nodes {
			nodes = append(nodes, node)
		}
	}
	nodes = append(nodes, core.NewLine())
	nodes = append(nodes, core.From())
	nodes = append(nodes, core.Space())
	nodes = append(nodes, state.from)
	if state.join != nil {
		nodes = append(nodes, core.NewLine())
		join := state.join.(core.NodeListNode)
		for i, node := range join.Nodes {
			if i > 1 {
				nodes = append(nodes, node)
			}
		}
	}
	if state.where != nil {
		nodes = append(nodes, core.NewLine())
		where := state.where.(core.NodeListNode)
		for i, node := range where.Nodes {
			if i > 1 {
				nodes = append(nodes, node)
			}
		}
	}
	if state.groupby != nil {
		nodes = append(nodes, core.NewLine())
		nodes = append(nodes, core.GroupBy())
		nodes = append(nodes, core.Space())
		groupby := state.groupby.(core.NodeListNode)
		for _, node := range groupby.Nodes {
			nodes = append(nodes, node)
		}
	}
	if state.orderby != nil {
		nodes = append(nodes, core.NewLine())
		orderby := state.orderby.(core.NodeListNode)
		for i, node := range orderby.Nodes {
			if i > 1 {
				nodes = append(nodes, node)
			}
		}
	}
	if state.limit != nil {
		nodes = append(nodes, core.NewLine())
		limit := state.limit.(core.NodeListNode)
		for i, node := range limit.Nodes {
			if i > 1 {
				nodes = append(nodes, node)
			}
		}
	}
	if parens {
		nodes = append(nodes, core.RightBracket())
	}
	return &core.NodeListNode{Nodes: nodes}
}
