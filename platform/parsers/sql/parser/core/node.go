// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package core

import (
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/util"
	"strings"
)

type Node interface {
	String() string
	Children() []Node
}

func ToTokenNodeMust(n Node) TokenNode {
	if tokenNode, ok := n.(TokenNode); ok {
		return tokenNode
	}
	panic("not a TokenNode")
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

func (n NodeListNode) N() int {
	return len(n.Nodes)
}

func (n NodeListNode) TrimLeft() NodeListNode {
	// eat spaces and new lines
	for len(n.Nodes) > 0 {
		if node, ok := n.Nodes[0].(TokenNode); ok && (node.Value() == " " || node.Value() == "\n") {
				n.Nodes = n.Nodes[1:]
		}
	}
		break
	}
	return n
}

func (n NodeListNode) TrimRight() NodeListNode {
	// eat spaces and new lines
	for len(n.Nodes) > 0 {
	    lastIndex := len(n.Nodes) - 1
		if node, ok := n.Nodes[lastIndex].(TokenNode); ok && (node.Value() == " " || node.Value() == "\n") {
				n.Nodes = n.Nodes[:lastIndex]
				continue
		}
		break
	}
	return n
}

func (n NodeListNode) Trim() NodeListNode {
	return n.TrimLeft().TrimRight()
}

func (n NodeListNode) EatFirst() NodeListNode {

	if len(n.Nodes) == 0 {
		return n
	}
	return NodeListNode{Nodes: n.Nodes[1:]}
}

type TokenNode struct {
	Token core.Token
}

func NewTokenNode(rawValue string) TokenNode {
	return TokenNode{
		Token: core.Token{
			RawValue: rawValue,
		},
	}
}

func NewTokenNodeSingleQuote(value string) TokenNode {
	return NewTokenNode(util.SingleQuote(value))
}

func (n TokenNode) Value() string {
	return n.Token.RawValue
}

func (n TokenNode) ValueUpper() string {
	return strings.ToUpper(n.Value())
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

type Pipe = []Node

func NewPipe(nodes ...Node) Pipe {
	return nodes
}

func Add(pipe *Pipe, nodes ...Node) {
	*pipe = append(*pipe, nodes...)
}
