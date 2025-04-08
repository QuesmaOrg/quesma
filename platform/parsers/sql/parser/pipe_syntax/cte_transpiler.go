// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package pipe_syntax

import (
	"fmt"
	lexercore "github.com/QuesmaOrg/quesma/platform/parsers/sql/lexer/core"
	"github.com/QuesmaOrg/quesma/platform/parsers/sql/parser/core"
	"slices"
	"strings"
)

type parsedQuery struct {
	name       string
	selectNode core.Node
	from       core.Node
	join       core.Node
	where      core.Node
	orderBy    core.Node
	limit      core.Node
	groupBy    core.Node
}

func compactIf(elems []parsedQuery, fn func(current, next parsedQuery) (parsedQuery, bool)) []parsedQuery {
	if len(elems) < 2 {
		return elems
	}

	var res []parsedQuery
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

func TranspileToCTE(node core.Node) {
	TransformPipeNodes(node, func(pipeNode *PipeNode) core.Node {

		// The first element is special.

		firstQuery := parsedQuery{}

		if pipeNodeList, ok := pipeNode.BeforePipe.(core.NodeListNode); ok {

			pipeNodeList = pipeNodeList.TrimLeft()

			if strings.ToUpper(pipeNodeList.Nodes[0].(core.TokenNode).Token.RawValue) == "FROM" {
				firstQuery.from = core.NodeListNode{Nodes: pipeNodeList.Nodes[1:]}.Trim()
			} else {
				firstQuery.from = pipeNodeList.Trim()
			}
		} else {
			firstQuery.from = core.NodeListNode{Nodes: []core.Node{pipeNode.BeforePipe}}.Trim()
		}

		var queries []parsedQuery

		queries = append(queries, firstQuery)

		for _, pipe := range pipeNode.Pipes {
			if pipeNodeList, ok := pipe.(core.NodeListNode); ok {

				pipeNodeList = pipeNodeList.TrimLeft()

				firstToken := pipeNodeList.Nodes[0].(core.TokenNode).Token.RawValue

				// TODO add proper error handling
				if firstToken != "|>" {
					continue
					// here we should return an error
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
					queries = append(queries, parsedQuery{
						join: pipeNode,
					})
					continue
				}

				// eat the command name
				pipeNodeList = pipeNodeList.EatFirst()
				pipeNodeList = pipeNodeList.TrimLeft()

				switch name {

				case "WHERE":
					queries = append(queries, parsedQuery{
						where: pipeNodeList.TrimRight(),
					})

				case "AGGREGATE":

					var groupBy []core.Node
					var selectNodes []core.Node

					groupByPart := false
					pipeNodeList = pipeNodeList.TrimRight()
					for _, groupByNode := range pipeNodeList.Nodes {
						if tokenNode, ok := groupByNode.(core.TokenNode); ok && strings.ToUpper(tokenNode.Token.RawValue) == "GROUP BY" {
							groupByPart = true
							continue
						}

						if !groupByPart {
							selectNodes = append(selectNodes, groupByNode)
						} else {
							groupBy = append(groupBy, groupByNode)
						}
					}

					groupByNodeList := core.NodeListNode{Nodes: groupBy}.Trim()

					element := parsedQuery{
						groupBy: groupByNodeList,
					}

					var allNodes []core.Node
					allNodes = slices.Clone(groupByNodeList.Nodes)
					allNodes = append(allNodes, core.TokenNode{Token: lexercore.Token{RawValue: ","}})
					allNodes = append(allNodes, core.TokenNode{Token: lexercore.Token{RawValue: " "}})
					allNodes = append(allNodes, selectNodes...)
					element.selectNode = core.NodeListNode{Nodes: allNodes}

					queries = append(queries, element)

				case "ORDER BY":
					queries = append(queries, parsedQuery{
						orderBy: pipeNodeList.TrimRight(),
					})

				case "SELECT":
					queries = append(queries, parsedQuery{
						selectNode: pipeNodeList.TrimRight(),
					})

				case "EXTEND":
					var allNodes []core.Node
					allNodes = []core.Node{core.TokenNode{Token: lexercore.Token{RawValue: "*"}}}
					allNodes = append(allNodes, core.TokenNode{Token: lexercore.Token{RawValue: ","}})
					allNodes = append(allNodes, core.TokenNode{Token: lexercore.Token{RawValue: " "}})
					allNodes = append(allNodes, pipeNodeList.Nodes...)
					queries = append(queries, parsedQuery{
						selectNode: core.NodeListNode{Nodes: allNodes},
					})

				case "LIMIT":
					queries = append(queries, parsedQuery{
						limit: pipeNodeList.TrimRight(),
					})

				default:
					fmt.Println("Unknown pipe command: ", name)
					// TODO we should return an error here
				}
			}
		}

		// name them
		for k := range queries {
			queries[k].name = fmt.Sprintf("%d", k+1)
		}

		queries = compactIf(queries, func(current, next parsedQuery) (parsedQuery, bool) {

			if current.orderBy == nil && next.orderBy != nil && next.from == nil && next.join == nil && next.where == nil && next.groupBy == nil && next.limit == nil && next.selectNode == nil {
				current.orderBy = next.orderBy
				current.name = fmt.Sprintf("%s_%s", current.name, next.name)
				return current, true

			}

			return current, false
		})

		queries = compactIf(queries, func(current, next parsedQuery) (parsedQuery, bool) {

			if current.limit == nil && next.limit != nil && next.from == nil && next.join == nil && next.where == nil && next.groupBy == nil && next.orderBy == nil && next.selectNode == nil {
				current.limit = next.limit
				current.name = fmt.Sprintf("%s_%s", current.name, next.name)
				return current, true

			}

			return current, false
		})

		// TODO add compaction  WHERE, COLUMNS

		// prefix the names
		for k := range queries {
			queries[k].name = fmt.Sprintf("_oql_pipe_%s", queries[k].name)
		}

		// rendering
		b := NewNodeBuilder()

		b.Add("WITH", " ")

		var prevName string
		var addComma bool
		for _, el := range queries {
			if addComma {
				b.Add(",", "\n")
			}

			if prevName != "" {
				el.from = core.NodeListNode{Nodes: []core.Node{core.TokenNode{Token: lexercore.Token{RawValue: prevName}}}}
			}

			b.Add("\n", el.name, " ", "AS", "\n", el.render())

			prevName = el.name
			addComma = true
		}
		b.Add("\n\n", "-- main query", "\n")

		b.Add("SELECT", " ", "*", " ", "FROM", " ", queries[len(queries)-1].name)

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
			builder.nodes = append(builder.nodes, core.TokenNode{Token: lexercore.Token{RawValue: t}})
		case core.Node:
			builder.nodes = append(builder.nodes, t)
		default:
			fmt.Printf("Unknown node type: %T\n", t)
			panic("Unknown node type")
		}

	}

	return builder
}

func (state parsedQuery) render() core.Node {

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

	if state.groupBy != nil {
		b.Add("\n")
		b.Add(" ", "GROUP BY", " ", state.groupBy)
	}

	if state.orderBy != nil {
		b.Add("\n")
		b.Add(" ", "ORDER BY", " ", state.orderBy)
	}

	if state.limit != nil {
		b.Add("\n")
		b.Add(" ", "LIMIT", " ", state.limit)
	}

	b.Add("\n", ")")

	return &core.NodeListNode{Nodes: b.Nodes()}
}
