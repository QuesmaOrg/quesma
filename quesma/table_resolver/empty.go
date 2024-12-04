// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"fmt"
	"quesma/frontend_connectors"
)

type EmptyTableResolver struct {
	Decisions          map[string]*frontend_connectors.Decision
	RecentDecisionList []frontend_connectors.PatternDecisions
	PipelinesList      []string
}

func NewEmptyTableResolver() *EmptyTableResolver {
	return &EmptyTableResolver{
		Decisions: make(map[string]*frontend_connectors.Decision),
	}
}

func (r *EmptyTableResolver) Resolve(pipeline string, indexPattern string) *frontend_connectors.Decision {
	d, ok := r.Decisions[indexPattern]
	if ok {
		return d
	}
	msg := fmt.Sprintf("Could not resolve pattern %v. Fix you test setup first.", indexPattern)
	return &frontend_connectors.Decision{
		Err:          fmt.Errorf("%s", msg),
		Reason:       msg,
		ResolverName: "EmptyTableResolver.Resolve",
	}
}

func (r *EmptyTableResolver) RecentDecisions() []frontend_connectors.PatternDecisions {
	return r.RecentDecisionList
}

func (r *EmptyTableResolver) Pipelines() []string {
	return r.PipelinesList
}

func (r *EmptyTableResolver) Start() {
}

func (r *EmptyTableResolver) Stop() {
}
