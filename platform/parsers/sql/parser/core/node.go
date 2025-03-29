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

func NewTokenNode(rawValue string) TokenNode {
	return TokenNode{
		Token: core.Token{
			RawValue: rawValue,
		},
	}
}

func (n TokenNode) ValueUpper() string {
	return strings.ToUpper(n.Token.RawValue)
}

func Concat() TokenNode {
	return NewTokenNode("concat")
}

func FormatDateTime() TokenNode {
	return NewTokenNode("formatDateTime")
}

func ToStartOfInterval() TokenNode {
	return NewTokenNode("toStartOfInterval")
}

func PipeToken() TokenNode {
	return NewTokenNode("|>")
}

func Space() TokenNode {
	return NewTokenNode(" ")
}

func Comma() TokenNode {
	return NewTokenNode(",")
}

func Regexp() TokenNode {
	return NewTokenNode("REGEXP")
}

func Then() TokenNode {
	return NewTokenNode("THEN")
}

func NewTokenNodeSingleQuote(value string) TokenNode {
	return NewTokenNode(util.SingleQuote(value))
}

func Plus() TokenNode {
	return NewTokenNode("+")
}

func Interval() TokenNode {
	return NewTokenNode("INTERVAL")
}

func LeftBracket() TokenNode {
	return NewTokenNode("(")
}

func Limit() TokenNode {
	return NewTokenNode("LIMIT")
}

func NewLine() TokenNode {
	return NewTokenNode("\n")
}

func GroupBy() TokenNode {
	return NewTokenNode("GROUP BY")
}

func Aggregate() TokenNode {
	return NewTokenNode("AGGREGATE")
}

func RightBracket() TokenNode {
	return NewTokenNode(")")
}

func Extend() TokenNode {
	return NewTokenNode("EXTEND")
}

func Else() TokenNode {
	return NewTokenNode("ELSE")
}

func QuesmaEnrichValue() TokenNode {
	return NewTokenNode("quesma_enrich.value")
}

func QuesmaEnrich() TokenNode {
	return NewTokenNode("quesma_enrich")
}

func QuesmaEnrichKey() TokenNode {
	return NewTokenNode("quesma_enrich.key")
}

func EnrichType() TokenNode {
	return NewTokenNode("enrich_type")
}

func As() TokenNode {
	return NewTokenNode("AS")
}

func And() TokenNode {
	return NewTokenNode("AND")
}

func On() TokenNode {
	return NewTokenNode("ON")
}

func Select() TokenNode {
	return NewTokenNode("SELECT")
}

func From() TokenNode {
	return NewTokenNode("FROM")
}

func When() TokenNode {
	return NewTokenNode("WHEN")
}

func Equals() TokenNode {
	return NewTokenNode("=")
}

func Case() TokenNode {
	return NewTokenNode("CASE")
}

func LeftJoin() TokenNode {
	return NewTokenNode("LEFT JOIN")
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

func Add(pipe Pipe, nodes ...Node) {
	pipe = append(pipe, nodes...)
}
