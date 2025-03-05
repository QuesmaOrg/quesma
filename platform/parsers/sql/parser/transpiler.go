package main

import (
	"fmt"
	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
	"strings"
)

type TranspileState struct {
	selectNode   core.Node
	from         core.Node
	join         core.Node
	where        core.Node
	orderby      core.Node
	limit        core.Node
	lastPriority int
}

func Transpile(node core.Node) core.Node {
	state := TranspileState{}

	pipeNode, ok := node.(*core.PipeNode)
	if !ok {
		return node
	}

	if pipeNodeList, ok := pipeNode.BeforePipe.(core.NodeListNode); ok {
		if pipeNodeList.Nodes[0].(core.TokenNode).Token.RawValue == "FROM" {
			state.from = pipeNodeList.Nodes[2]
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
					state = TranspileState{from: renderState(state)}
				}
				state.join = pipeNodeList
				state.lastPriority = 2
			case "WHERE":
				if state.lastPriority >= 3 {
					state = TranspileState{from: renderState(state)}
				}
				state.where = pipeNodeList
				state.lastPriority = 3
			case "ORDER BY":
				if state.lastPriority >= 9 {
					state = TranspileState{from: renderState(state)}
				}
				state.orderby = pipeNodeList
				state.lastPriority = 9
			case "SELECT":
				if state.lastPriority >= 7 {
					state = TranspileState{from: renderState(state)}
				}
				state.selectNode = pipeNodeList
				state.lastPriority = 7
			case "LIMIT":
				if state.lastPriority >= 10 {
					state = TranspileState{from: renderState(state)}
				}
				state.limit = pipeNodeList
				state.lastPriority = 10
			default:
				fmt.Println("Unknown pipe: ", name)
			}
		}
	}

	return renderState(state)
}

func renderState(state TranspileState) core.Node {
	var nodes []core.Node

	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "SELECT"}})
	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
	if state.selectNode == nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "*"}})
	} else {
		selectNode := state.selectNode.(core.NodeListNode)
		for i, node := range selectNode.Nodes {
			if i > 3 {
				nodes = append(nodes, node)
			}
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

	return &core.NodeListNode{Nodes: nodes}
}
