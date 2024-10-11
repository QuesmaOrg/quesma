// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import "fmt"

type EmptyTableResolver struct {
	Decisions          map[string]*Decision
	RecentDecisionList []PatternDecisions
	PipelinesList      []string
}

func NewEmptyTableResolver() *EmptyTableResolver {
	return &EmptyTableResolver{
		Decisions: make(map[string]*Decision),
	}
}

func (r *EmptyTableResolver) Resolve(pipeline string, indexPattern string) *Decision {
	d, ok := r.Decisions[indexPattern]
	if ok {
		return d
	}
	msg := fmt.Sprintf("Could not resolve pattern %v. Fix you test setup first.", indexPattern)
	return &Decision{
		Err:          fmt.Errorf("%s", msg),
		Reason:       msg,
		ResolverName: "EmptyTableResolver.Resolve",
	}
}

func (r *EmptyTableResolver) RecentDecisions() []PatternDecisions {
	return r.RecentDecisionList
}

func (r *EmptyTableResolver) Pipelines() []string {
	return r.PipelinesList
}

func (r *EmptyTableResolver) Start() {
}

func (r *EmptyTableResolver) Stop() {
}
