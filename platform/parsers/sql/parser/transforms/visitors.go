// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package transforms

import "github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"

func VisitListNodes(node core.Node, visitor func(nodeListNode *core.NodeListNode) []core.Node) {
	for _, child := range node.Children() {
		VisitListNodes(child, visitor)
	}
	if nodeListNode, ok := node.(*core.NodeListNode); ok {
		newNodes := visitor(nodeListNode)
		nodeListNode.Nodes = newNodes
	}
}

func VisitTokenNodes(node core.Node, visitor func(tokenNode core.TokenNode)) {
	for _, child := range node.Children() {
		VisitTokenNodes(child, visitor)
	}
	if tokenNode, ok := node.(core.TokenNode); ok {
		visitor(tokenNode)
	}
}
