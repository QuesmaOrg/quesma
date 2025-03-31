// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// This file contains code derived from:
//
// 1. sqlparse (Copyright (c) 2016, Andi Albrecht)
//    Licensed under BSD-3-Clause License
//    https://github.com/andialbrecht/sqlparse/blob/38c065b86ac43f76ffd319747e57096ed78bfa63/LICENSE

package transforms

import (
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
)

// The following code block is based on grouping transformation from sqlparse:
// https://github.com/andialbrecht/sqlparse/blob/a801100e9843786a9139bebb97c951603637129c/sqlparse/engine/grouping.py#L56-L57

func GroupParenthesis(node core.Node) {
	TransformListNodes(node, func(nodeListNode *core.NodeListNode) []core.Node {
		parser := groupParenthesisParser{nodes: nodeListNode.Nodes, currentPos: 0}
		return parser.Parse(false)
	})
}

type groupParenthesisParser struct {
	nodes      []core.Node
	currentPos int
}

func (p *groupParenthesisParser) Parse(parenStarted bool) []core.Node {
	var newNodes []core.Node

	// This is the starting parenthesis of currently parsed group, handle it separately
	// to avoid infinite loop.
	if parenStarted && isOpeningParenthesis(p.nodes[p.currentPos]) {
		newNodes = append(newNodes, p.nodes[p.currentPos])
		p.currentPos++
	}

	for p.currentPos < len(p.nodes) {
		// Nested parenthesis, handle it recursively.
		if isOpeningParenthesis(p.nodes[p.currentPos]) {
			newNodes = append(newNodes, &core.NodeListNode{Nodes: p.Parse(true)})
			continue
		}

		// End of current group, stop consuming forward
		// (except if we are in the top-level iteration, parenStarted=false,
		// in order to handle cases like '(ab)(cd)').
		if parenStarted && isClosingParenthesis(p.nodes[p.currentPos]) {
			newNodes = append(newNodes, p.nodes[p.currentPos])
			p.currentPos++
			break
		}

		newNodes = append(newNodes, p.nodes[p.currentPos])
		p.currentPos++
	}

	return newNodes
}

func isOpeningParenthesis(node core.Node) bool {
	if tokenNode, ok := node.(core.TokenNode); ok {
		return tokenNode.Value() == "("
	}
	return false
}

func isClosingParenthesis(node core.Node) bool {
	if tokenNode, ok := node.(core.TokenNode); ok {
		return tokenNode.Value() == ")"
	}
	return false
}
