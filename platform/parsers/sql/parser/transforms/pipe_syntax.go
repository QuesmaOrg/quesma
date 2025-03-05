// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package transforms

import "github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"

func TransformPipeSyntax(node core.Node) core.Node {
	nodeList, ok := node.(*core.NodeListNode)
	if !ok {
		// TODO: this should recurse into the node generally
		return node
	}

	var beforePipe []core.Node

	var i int
	for i = 0; i < len(nodeList.Nodes); i++ {
		if tokenNode, ok := nodeList.Nodes[i].(core.TokenNode); ok {
			if tokenNode.Token.RawValue == "|>" {
				break
			}
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
		if tokenNode, ok := nodeList.Nodes[i].(core.TokenNode); ok {
			if tokenNode.Token.RawValue == "|>" {
				appendCurrentPipe()
			}
		}

		currentPipe = append(currentPipe, nodeList.Nodes[i])
	}
	appendCurrentPipe()

	for i, node := range beforePipe {
		beforePipe[i] = TransformPipeSyntax(node)
	}

	if len(pipes) == 0 {
		return node
	}

	return &core.PipeNode{
		BeforePipe: core.NodeListNode{Nodes: beforePipe},
		Pipes:      pipes,
	}
}
