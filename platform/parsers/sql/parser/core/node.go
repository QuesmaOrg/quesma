// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package core

import "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"

type Node interface {
	String() string
	Children() []Node
}

type NodeListNode struct {
	Nodes []Node
}

func (n NodeListNode) String() string {
	result := "NodeListNode[\n"
	for i, node := range n.Nodes {
		if i > 0 {
			result += ",\n"
		}
		result += node.String()
	}
	result += "\n]"
	return result
}

func (n NodeListNode) Children() []Node {
	return n.Nodes
}

type TokenNode struct {
	Token core.Token
}

func (n TokenNode) String() string {
	return "TokenNode[" + n.Token.String() + "]"
}

func (n TokenNode) Children() []Node {
	return []Node{}
}

func TokensToNode(tokens []core.Token) Node {
	var nodes []Node

	for _, token := range tokens {
		nodes = append(nodes, TokenNode{Token: token})
	}

	return &NodeListNode{Nodes: nodes}
}

type PipeNode struct {
	BeforePipe Node
	Pipes      []Node
}

func (n PipeNode) String() string {
	result := "PipeNode[\n"
	result += "BeforePipe: " + n.BeforePipe.String() + ",\n"
	result += "Pipes: ["
	for i, pipe := range n.Pipes {
		if i > 0 {
			result += ", "
		}
		result += pipe.String()
	}
	result += "]\n"
	result += "]"
	return result
}

func (n PipeNode) Children() []Node {
	children := []Node{n.BeforePipe}
	children = append(children, n.Pipes...)
	return children
}
