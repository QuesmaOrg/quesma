// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package pipe_syntax

import (
	"fmt"
	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
	"slices"
	"strings"
)

type pipeElement struct {
	name       string
	selectNode core.Node
	from       core.Node
	join       core.Node
	where      core.Node
	orderby    core.Node
	limit      core.Node
	groupby    core.Node
}

func foldIf(elems []pipeElement, fn func(current, next pipeElement) (pipeElement, bool)) []pipeElement {
	if len(elems) < 2 {
		return elems
	}

	var res []pipeElement
	i := 0

	for i < len(elems)-1 {
		el, ok := fn(elems[i], elems[i+1])
		if ok {
			res = append(res, el)
			i += 2
		} else {
			res = append(res, elems[i])
			i++
		}
	}

	if i == len(elems)-1 {
		res = append(res, elems[i])
	}

	return res
}

func TranspileCTE(node core.Node) {
	TransformPipeNodes(node, func(pipeNode *PipeNode) core.Node {

		// The first element is special.

		firstElement := pipeElement{}

		if pipeNodeList, ok := pipeNode.BeforePipe.(core.NodeListNode); ok {

			pipeNodeList = pipeNodeList.TrimLeft()

			if strings.ToUpper(pipeNodeList.Nodes[0].(core.TokenNode).Token.RawValue) == "FROM" {
				firstElement.from = core.NodeListNode{Nodes: pipeNodeList.Nodes[1:]}.Trim()
			} else {
				firstElement.from = pipeNodeList.Trim()
			}
		} else {
			firstElement.from = core.NodeListNode{Nodes: []core.Node{pipeNode.BeforePipe}}.Trim()
		}

		var elements []pipeElement

		elements = append(elements, firstElement)

		for _, pipe := range pipeNode.Pipes {
			if pipeNodeList, ok := pipe.(core.NodeListNode); ok {

				pipeNodeList = pipeNodeList.TrimLeft()

				firstToken := pipeNodeList.Nodes[0].(core.TokenNode).Token.RawValue

				// TODO add proper error handling
				if firstToken != "|>" {
					panic("It should be a pipe operator")
				}

				pipeNodeList = pipeNodeList.EatFirst()
				pipeNodeList = pipeNodeList.TrimLeft()

				if len(pipeNodeList.Nodes) == 0 {
					// just a corner case here
					continue
				}

				name := strings.ToUpper(pipeNodeList.Nodes[0].(core.TokenNode).Token.RawValue)

				// JOIN, CROSS JOIN, LEFT OUTER JOIN etc.
				if strings.HasSuffix(name, "JOIN") {
					elements = append(elements, pipeElement{
						join: pipeNode,
					})
					continue
				}

				// eat the command name
				pipeNodeList = pipeNodeList.EatFirst()
				pipeNodeList = pipeNodeList.TrimLeft()

				switch name {

				case "WHERE":
					elements = append(elements, pipeElement{
						where: pipeNodeList.TrimRight(),
					})

				case "AGGREGATE":

					var groupby []core.Node
					var selectNodes []core.Node

					groupbyPart := false
					pipeNodeList = pipeNodeList.TrimRight()
					for _, groupByNode := range pipeNodeList.Nodes {
						if tokenNode, ok := groupByNode.(core.TokenNode); ok && strings.ToUpper(tokenNode.Token.RawValue) == "GROUP BY" {
							groupbyPart = true
							continue
						}

						if !groupbyPart {
							selectNodes = append(selectNodes, groupByNode)
						} else {
							groupby = append(groupby, groupByNode)
						}
					}

					groupByNodeList := core.NodeListNode{Nodes: groupby}.Trim()

					element := pipeElement{
						groupby: groupByNodeList,
					}

					var allNodes []core.Node
					allNodes = slices.Clone(groupByNodeList.Nodes)
					allNodes = append(allNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
					allNodes = append(allNodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
					allNodes = append(allNodes, selectNodes...)
					element.selectNode = core.NodeListNode{Nodes: allNodes}

					elements = append(elements, element)

				case "ORDER BY":
					elements = append(elements, pipeElement{
						orderby: pipeNodeList.TrimRight(),
					})

				case "SELECT":
					elements = append(elements, pipeElement{
						selectNode: pipeNodeList.TrimRight(),
					})

				case "EXTEND":
					var allNodes []core.Node
					allNodes = []core.Node{core.TokenNode{Token: lexer_core.Token{RawValue: "*"}}}
					allNodes = append(allNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
					allNodes = append(allNodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
					allNodes = append(allNodes, pipeNodeList.Nodes...)
					elements = append(elements, pipeElement{
						selectNode: core.NodeListNode{Nodes: allNodes},
					})

				case "LIMIT":
					elements = append(elements, pipeElement{
						limit: pipeNodeList,
					})

				default:
					fmt.Println("Unknown pipe command: ", name)
				}
			}
		}

		// name them
		for k := range elements {
			elements[k].name = fmt.Sprintf("_oql_pipe_%d", k+1)
		}

		elements = foldIf(elements, func(current, next pipeElement) (pipeElement, bool) {

			if current.orderby == nil && next.orderby != nil && next.from == nil && next.join == nil && next.where == nil && next.groupby == nil && next.limit == nil && next.selectNode == nil {
				current.orderby = next.orderby
				return current, true

			}

			return current, false
		})

		elements = foldIf(elements, func(current, next pipeElement) (pipeElement, bool) {

			if current.limit == nil && next.limit != nil && next.from == nil && next.join == nil && next.where == nil && next.groupby == nil && next.orderby == nil && next.selectNode == nil {
				current.limit = next.limit
				return current, true

			}

			return current, false
		})

		// TODO add compaction ORDER BY, LIMIT, WHERE, COLUMNS

		// rendering
		b := NewNodeBuilder()

		b.Add("WITH", " ")

		var prevName string
		var addComma bool
		for _, el := range elements {
			if addComma {
				b.Add(",", "\n")
			}

			if prevName != "" {
				el.from = core.NodeListNode{Nodes: []core.Node{core.TokenNode{Token: lexer_core.Token{RawValue: prevName}}}}
			}

			b.Add("\n", el.name, " ", "AS", "\n", el.render())

			prevName = el.name
			addComma = true
		}
		b.Add("\n\n", "-- main query", "\n")

		b.Add("SELECT", " ", "*", " ", "FROM", " ", elements[len(elements)-1].name)

		return core.NodeListNode{Nodes: b.Nodes()}
	})
}

type NodeBuilder struct {
	nodes []core.Node
}

func NewNodeBuilder() *NodeBuilder {
	return &NodeBuilder{
		nodes: []core.Node{},
	}
}

func (builder *NodeBuilder) Nodes() []core.Node {
	return builder.nodes
}

func (builder *NodeBuilder) Add(nodes ...any) *NodeBuilder {

	for _, node := range nodes {

		switch t := node.(type) {

		case string:
			builder.nodes = append(builder.nodes, core.TokenNode{Token: lexer_core.Token{RawValue: t}})

		case core.Node:

			builder.nodes = append(builder.nodes, t)

		case core.NodeListNode:
			builder.nodes = append(builder.nodes, t.Nodes...)

		default:
			fmt.Printf("Unknown node type: %T\n", t)
			panic("Unknown node type")
		}

	}

	return builder
}

func (state pipeElement) render() core.Node {

	b := NewNodeBuilder()

	b.Add("(", "\n", " ", "SELECT", " ")

	if state.selectNode == nil {
		b.Add("*")
	} else {
		b.Add(state.selectNode)
	}

	b.Add("\n")
	b.Add(" ", "FROM", " ", state.from)

	if state.join != nil {
		b.Add("\n")
		b.Add(" ", "JOIN", " ", state.join)
	}

	if state.where != nil {
		b.Add("\n")
		b.Add(" ", "WHERE", " ", state.where)
	}

	if state.groupby != nil {
		b.Add("\n")
		b.Add(" ", "GROUP BY", " ", state.groupby)
	}

	if state.orderby != nil {
		b.Add("\n")
		b.Add(" ", "ORDER BY", " ", state.orderby)
	}

	if state.limit != nil {
		b.Add("\n")
		b.Add(" ", "LIMIT", " ", state.limit)
	}

	b.Add("\n", ")")

	return &core.NodeListNode{Nodes: b.Nodes()}
}
