// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/v2/core/diag"
)

type Dependencies interface {
	PhoneHomeAgent() diag.PhoneHomeClient
	DebugInfoCollector() diag.DebugInfoCollector
	Logger() QuesmaLogger
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
	Name      string
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

	id := fmt.Sprintf("%T(%p)", a, a)
	name := id
	if identifiable, ok := a.(InstanceNamer); ok {
		name = identifiable.InstanceName()
	}

	node := &ComponentTreeNode{
		Id:        id,
		Name:      name,
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
	logger             QuesmaLogger
}

func (d *DependenciesImpl) PhoneHomeAgent() diag.PhoneHomeClient {
	return d.phoneHomeAgent
}

func (d *DependenciesImpl) DebugInfoCollector() diag.DebugInfoCollector {
	return d.debugInfoCollector
}

func (d *DependenciesImpl) Logger() QuesmaLogger {
	return d.logger
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

func (d *DependenciesImpl) SetLogger(logger QuesmaLogger) {
	d.logger = logger
}

func (d *DependenciesImpl) Clone() *DependenciesImpl {
	return &DependenciesImpl{
		phoneHomeAgent:     d.phoneHomeAgent,
		debugInfoCollector: d.debugInfoCollector,
		logger:             d.logger,
	}
}

func EmptyDependencies() *DependenciesImpl {
	return &DependenciesImpl{
		phoneHomeAgent:     diag.NewPhoneHomeEmptyAgent(),
		debugInfoCollector: diag.EmptyDebugInfoCollector(),

		logger: EmptyQuesmaLogger(),
	}
}

const traceDependencyInjection bool = true

// InjectDependenciesInto injects dependencies into a component. This is indented to use in Quesma building process only.
func (d *DependenciesImpl) InjectDependenciesInto(component any) {

	// TODO fmt for now. Later we can use logger. We need to move logger to the V2 module.

	var trace func(a ...any)

	if traceDependencyInjection {
		prefix := fmt.Sprintf("Dependency injection into %T :", component)
		trace = func(a ...any) {
			d.logger.Info().Msgf("%s%s", prefix, fmt.Sprint(a...))
			//fmt.Println(prefix, fmt.Sprint(a...))
		}
	} else {
		trace = func(a ...any) {}
	}

	if target, ok := component.(DependenciesSetter); ok {
		deps := d

		if named, ok := component.(InstanceNamer); ok {
			// We have a named component. We can use to inject sub logger here.

			deps = d.Clone()
			deps.SetLogger(deps.Logger().WithComponent(named.InstanceName()))
			trace("Injecting dependencies into", named.InstanceName())
		}

		target.SetDependencies(deps)
		trace("OK - Injected Dependencies")

	} else {
		trace("SKIP - No dependencies to inject. It doesn't implement DependenciesSetter interface.")
	}
}
