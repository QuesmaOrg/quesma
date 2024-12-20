// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"fmt"
	"quesma_v2/core/diag"
)

type Dependencies interface {
	PhoneHomeAgent() diag.PhoneHomeClient
	DebugInfoCollector() diag.DebugInfoCollector

	InjectDependenciesInto(a any)
}

// Here are interfaces that are used to inject dependencies into structs.
// Component that require a dependency should implement the corresponding interface.
//

type DependenciesSetter interface {
	SetDependencies(deps Dependencies)
}

//

type ChildComponentProvider interface {
	GetChildComponents() []any
}

type ComponentTreeNode struct {
	Id        string
	Level     int
	Component any
	Children  []*ComponentTreeNode
}

func (n *ComponentTreeNode) walk(f func(*ComponentTreeNode)) {
	f(n)
	for _, child := range n.Children {
		child.walk(f)
	}
}

type ComponentTreeBuilder struct {
	visited map[any]*ComponentTreeNode
}

func NewComponentToInitializeProviderBuilder() *ComponentTreeBuilder {
	return &ComponentTreeBuilder{
		visited: make(map[any]*ComponentTreeNode),
	}
}

func (b *ComponentTreeBuilder) buildComponentTree(level int, a any) *ComponentTreeNode {

	// cycle detection
	// TODO add detection if the a is hashable
	if v, ok := b.visited[a]; ok {
		return v
	}

	node := &ComponentTreeNode{
		Id:        fmt.Sprintf("%T(%p)", a, a),
		Children:  make([]*ComponentTreeNode, 0),
		Component: a,
		Level:     level,
	}

	b.visited[a] = node

	if provider, ok := a.(ChildComponentProvider); ok {
		for _, child := range provider.GetChildComponents() {
			childNode := b.buildComponentTree(level+1, child)
			node.Children = append(node.Children, childNode)
		}
	}

	return node
}

func (b *ComponentTreeBuilder) BuildComponentTree(a any) *ComponentTreeNode {
	return b.buildComponentTree(0, a)
}

// Dependencies is a struct that contains all the dependencies that can be injected during Quesma building.

type DependenciesImpl struct {
	phoneHomeAgent     diag.PhoneHomeClient
	debugInfoCollector diag.DebugInfoCollector
}

func (d *DependenciesImpl) PhoneHomeAgent() diag.PhoneHomeClient {
	return d.phoneHomeAgent
}

func (d *DependenciesImpl) DebugInfoCollector() diag.DebugInfoCollector {
	return d.debugInfoCollector
}

func NewDependencies() *DependenciesImpl {
	return EmptyDependencies()
}

func (d *DependenciesImpl) SetPhoneHomeAgent(phoneHomeAgent diag.PhoneHomeClient) {
	d.phoneHomeAgent = phoneHomeAgent
}

func (d *DependenciesImpl) SetDebugInfoCollector(debugInfoCollector diag.DebugInfoCollector) {
	d.debugInfoCollector = debugInfoCollector
}

func EmptyDependencies() *DependenciesImpl {
	return &DependenciesImpl{
		phoneHomeAgent:     diag.NewPhoneHomeEmptyAgent(),
		debugInfoCollector: diag.EmptyDebugInfoCollector(),
	}
}

const traceDependencyInjection bool = false

// InjectDependenciesInto injects dependencies into a component. This is indented to use in Quesma building process only.
func (d *DependenciesImpl) InjectDependenciesInto(a any) {

	// TODO fmt for now. Later we can use logger. We need to move logger to the V2 module.

	var trace func(a ...any)

	if traceDependencyInjection {
		prefix := fmt.Sprintf("Dependency injection into %T :", a)
		trace = func(a ...any) {
			fmt.Println(prefix, fmt.Sprint(a...))
		}
	} else {
		trace = func(a ...any) {}
	}

	if injector, ok := a.(DependenciesSetter); ok {
		injector.SetDependencies(d)
		trace("OK - Injected Dependencies")

	} else {
		trace("SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.")
	}
}
