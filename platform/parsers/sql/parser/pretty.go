// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package main

import (
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/transforms"
)

// PrettyPrint provides a formatted view of the Node tree.
func PrettyPrint(node core.Node) string {
	return prettyPrintNode(node, 0)
}

func prettyPrintNode(node core.Node, indent int) string {
	// pad returns an indentation string of two spaces per indent level.
	pad := func(n int) string {
		s := ""
		for i := 0; i < n; i++ {
			s += "  "
		}
		return s
	}

	// Handle different node types.
	switch n := node.(type) {
	// For NodeListNode (both pointer and non-pointer versions), iterate through the child nodes.
	case core.NodeListNode:
		s := "(\n" + pad(indent)
		for _, child := range n.Nodes {
			s += prettyPrintNode(child, indent+1)
		}
		s += "\n)"
		return s
	case *core.NodeListNode:
		s := "(\n" + pad(indent)
		for _, child := range n.Nodes {
			s += prettyPrintNode(child, indent+1)
		}
		s += "\n)"
		return s

	// For PipeNode (both pointer and non-pointer versions), show BeforePipe and Pipes fields.
	case transforms.PipeNode:
		s := pad(indent) + "PipeNode {\n"
		s += pad(indent+1) + "BeforePipe:\n" + prettyPrintNode(n.BeforePipe, indent+2) + "\n"
		s += pad(indent+1) + "Pipes: [\n"
		for _, p := range n.Pipes {
			s += prettyPrintNode(p, indent+2) + "\n"
		}
		s += pad(indent+1) + "]\n"
		s += pad(indent) + "}"
		return s
	case *transforms.PipeNode:
		s := pad(indent) + "PipeNode {\n"
		s += pad(indent+1) + "BeforePipe:\n" + prettyPrintNode(n.BeforePipe, indent+2) + "\n"
		s += pad(indent+1) + "Pipes: [\n"
		for _, p := range n.Pipes {
			s += prettyPrintNode(p, indent+2) + "\n"
		}
		s += pad(indent+1) + "]\n"
		s += pad(indent) + "}"
		return s

	// For TokenNode (both pointer and non-pointer), print the token's raw value.
	case core.TokenNode:
		return n.Token.RawValue
	case *core.TokenNode:
		return n.Token.RawValue

	// Default: fallback to the node's String() method, indenting each line.
	default:
		// Split the result into lines and add indentation.
		out := ""
		for _, line := range splitLines(node.String()) {
			out += pad(indent) + line + "\n"
		}
		// Remove the trailing newline.
		if len(out) > 0 {
			out = out[:len(out)-1]
		}
		return out
	}
}

// splitLines splits a string at newline characters.
func splitLines(s string) []string {
	var lines []string
	curr := ""
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, curr)
			curr = ""
		} else {
			curr += string(s[i])
		}
	}
	// Add the last line if non-empty.
	if curr != "" {
		lines = append(lines, curr)
	}
	return lines
}
