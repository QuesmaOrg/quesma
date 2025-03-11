// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package pipe_syntax

import (
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/transforms"
)

type PipeNode struct {
	BeforePipe core.Node
	Pipes      []core.Node
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

func (n PipeNode) Children() []core.Node {
	children := []core.Node{n.BeforePipe}
	children = append(children, n.Pipes...)
	return children
}

func GroupPipeSyntax(node core.Node) {
	transforms.TransformListNodes(node, func(nodeList *core.NodeListNode) []core.Node {
		var beforePipe []core.Node

		var i int
		for i = 0; i < len(nodeList.Nodes); i++ {
			if isPipeOperator(nodeList.Nodes[i]) {
				break
			}
			beforePipe = append(beforePipe, nodeList.Nodes[i])
		}

		var currentPipe []core.Node
		var pipes []core.Node

		appendCurrentPipe := func() {
			if len(currentPipe) > 0 {
				pipes = append(pipes, core.NodeListNode{Nodes: currentPipe})
				currentPipe = nil
			}
		}

		for ; i < len(nodeList.Nodes); i++ {
			if isPipeOperator(nodeList.Nodes[i]) {
				appendCurrentPipe()
			}

			currentPipe = append(currentPipe, nodeList.Nodes[i])
		}
		appendCurrentPipe()

		if len(pipes) == 0 {
			return nodeList.Nodes
		}

		return []core.Node{&PipeNode{
			BeforePipe: core.NodeListNode{Nodes: beforePipe},
			Pipes:      pipes,
		}}
	})
}

func isPipeOperator(node core.Node) bool {
	tokenNode, ok := node.(core.TokenNode)
	return ok && tokenNode.Token.RawValue == "|>"
}
