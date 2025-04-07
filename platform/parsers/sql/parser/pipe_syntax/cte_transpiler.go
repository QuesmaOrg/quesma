// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package pipe_syntax

import (
	"fmt"
	lexer_core "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
	"github.com/QuesmaOrg/quesma/platform/util"
	"slices"
	"strings"
)

type pipeElement struct {
	name         string
	selectNode   core.Node
	from         core.Node
	join         core.Node
	where        core.Node
	orderby      core.Node
	limit        core.Node
	groupby      core.Node
	lastPriority int
}

func TranspileCTE(node core.Node) {
	TransformPipeNodes(node, func(pipeNode *PipeNode) core.Node {

		firstElement := pipeElement{}

		if pipeNodeList, ok := pipeNode.BeforePipe.(core.NodeListNode); ok {
			if strings.ToUpper(pipeNodeList.Nodes[0].(core.TokenNode).Token.RawValue) == "FROM" {
				firstElement.from = core.NodeListNode{Nodes: pipeNodeList.Nodes[1:]}
			} else {
				firstElement.from = pipeNodeList
			}
		} else {
			firstElement.from = core.NodeListNode{Nodes: []core.Node{pipeNode.BeforePipe}}
		}

		var elements []pipeElement

		elements = append(elements, firstElement)

		for _, pipe := range pipeNode.Pipes {
			if pipeNodeList, ok := pipe.(core.NodeListNode); ok {
				name := strings.ToUpper(pipeNodeList.Nodes[2].(core.TokenNode).Token.RawValue)

				// JOIN, CROSS JOIN, LEFT OUTER JOIN etc.
				if strings.HasSuffix(name, "JOIN") {
					elements = append(elements, pipeElement{
						join: pipeNode,
					})
					continue
				}

				switch name {
				case "WHERE":
					elements = append(elements, pipeElement{
						where: pipeNodeList,
					})
				case "AGGREGATE":

					var groupby []core.Node
					var selectNodes []core.Node

					groupbyPart := false
					for _, node := range pipeNodeList.Nodes[3:] {
						if tokenNode, ok := node.(core.TokenNode); ok && strings.ToUpper(tokenNode.Token.RawValue) == "GROUP BY" {
							groupbyPart = true
							continue
						}

						if !groupbyPart {
							selectNodes = append(selectNodes, node)
						} else {
							groupby = append(groupby, node)
						}
					}

					element := pipeElement{
						groupby: core.NodeListNode{Nodes: groupby},
					}

					var allNodes []core.Node
					allNodes = slices.Clone(groupby)
					allNodes = append(allNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
					allNodes = append(allNodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
					allNodes = append(allNodes, selectNodes...)
					element.selectNode = core.NodeListNode{Nodes: allNodes}

					elements = append(elements, element)

				case "ORDER BY":

					elements = append(elements, pipeElement{
						orderby: pipeNodeList,
					})

				case "SELECT":
					elements = append(elements, pipeElement{
						selectNode: core.NodeListNode{Nodes: pipeNodeList.Nodes[3:]},
					})

				case "EXTEND":

					var allNodes []core.Node
					allNodes = []core.Node{core.TokenNode{Token: lexer_core.Token{RawValue: "*"}}}
					allNodes = append(allNodes, core.TokenNode{Token: lexer_core.Token{RawValue: ","}})
					allNodes = append(allNodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
					allNodes = append(allNodes, pipeNodeList.Nodes[3:]...)
					elements = append(elements, pipeElement{
						selectNode: core.NodeListNode{Nodes: allNodes},
					})

				case "LIMIT":

					elements = append(elements, pipeElement{
						limit: pipeNodeList,
					})

				default:
					fmt.Println("Unknown pipe: ", name)
				}
			}
		}

		// name it
		for k, _ := range elements {
			elements[k].name = fmt.Sprintf("pipe_%d", k)
		}

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

			b.Add(el.name, " ", "AS", " ", el.render(true))
			b.Add("\n")
			prevName = el.name
			addComma = true
		}
		b.Add("\n", "-- main query", "\n")

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
			fmt.Println("Unknown node type:", t)
			panic("Unknown node type")
		}

	}

	return builder
}

func (state pipeElement) render(parens bool) core.Node {
	var nodes []core.Node

	if parens {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "("}})
	}
	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "SELECT"}})
	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
	if state.selectNode == nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "*"}})
	} else {
		selectNode := state.selectNode.(core.NodeListNode)
		nodes = append(nodes, selectNode.Nodes...)
	}
	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "FROM"}})
	nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
	nodes = append(nodes, state.from)
	if state.join != nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
		join := state.join.(core.NodeListNode)
		nodes = util.AppendFromIdx2(nodes, join.Nodes)
	}
	if state.where != nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
		where := state.where.(core.NodeListNode)
		nodes = util.AppendFromIdx2(nodes, where.Nodes)
	}
	if state.groupby != nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "GROUP BY"}})
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: " "}})
		groupby := state.groupby.(core.NodeListNode)
		nodes = append(nodes, groupby.Nodes...)
	}
	if state.orderby != nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
		orderby := state.orderby.(core.NodeListNode)
		nodes = util.AppendFromIdx2(nodes, orderby.Nodes)
	}
	if state.limit != nil {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: "\n"}})
		limit := state.limit.(core.NodeListNode)
		nodes = util.AppendFromIdx2(nodes, limit.Nodes)
	}
	if parens {
		nodes = append(nodes, core.TokenNode{Token: lexer_core.Token{RawValue: ")"}})
	}
	return &core.NodeListNode{Nodes: nodes}
}
