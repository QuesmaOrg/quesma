// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package pipe_syntax

import "github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"

func TransformPipeNodes(node core.Node, visitor func(pipeNode *PipeNode) core.Node) {
	for _, child := range node.Children() {
		TransformPipeNodes(child, visitor)
	}
	if nodeListNode, ok := node.(*core.NodeListNode); ok {
		for i, child := range nodeListNode.Nodes {
			if pipeNode, ok := child.(*PipeNode); ok {
				nodeListNode.Nodes[i] = visitor(pipeNode)
			}
		}
	}
}
