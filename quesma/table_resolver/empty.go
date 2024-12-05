// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"fmt"
	"quesma_v2/core/mux"
)

type EmptyTableResolver struct {
	Decisions          map[string]*mux.Decision
	RecentDecisionList []mux.PatternDecisions
	PipelinesList      []string
}

func NewEmptyTableResolver() *EmptyTableResolver {
	return &EmptyTableResolver{
		Decisions: make(map[string]*mux.Decision),
	}
}

func (r *EmptyTableResolver) Resolve(pipeline string, indexPattern string) *mux.Decision {
	d, ok := r.Decisions[indexPattern]
	if ok {
		return d
	}
	msg := fmt.Sprintf("Could not resolve pattern %v. Fix you test setup first.", indexPattern)
	return &mux.Decision{
		Err:          fmt.Errorf("%s", msg),
		Reason:       msg,
		ResolverName: "EmptyTableResolver.Resolve",
	}
}

func (r *EmptyTableResolver) RecentDecisions() []mux.PatternDecisions {
	return r.RecentDecisionList
}

func (r *EmptyTableResolver) Pipelines() []string {
	return r.PipelinesList
}

func (r *EmptyTableResolver) Start() {
}

func (r *EmptyTableResolver) Stop() {
}
