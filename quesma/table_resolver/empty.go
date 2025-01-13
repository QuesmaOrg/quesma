// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/v2/core"
)

type EmptyTableResolver struct {
	Decisions          map[string]*quesma_api.Decision
	RecentDecisionList []quesma_api.PatternDecisions
	PipelinesList      []string
}

func NewEmptyTableResolver() *EmptyTableResolver {
	return &EmptyTableResolver{
		Decisions: make(map[string]*quesma_api.Decision),
	}
}

func (r *EmptyTableResolver) Resolve(pipeline string, indexPattern string) *quesma_api.Decision {
	d, ok := r.Decisions[indexPattern]
	if ok {
		return d
	}
	msg := fmt.Sprintf("Could not resolve pattern %v. Fix you test setup first.", indexPattern)
	return &quesma_api.Decision{
		Err:          fmt.Errorf("%s", msg),
		Reason:       msg,
		ResolverName: "EmptyTableResolver.Resolve",
	}
}

func (r *EmptyTableResolver) RecentDecisions() []quesma_api.PatternDecisions {
	return r.RecentDecisionList
}

func (r *EmptyTableResolver) Pipelines() []string {
	return r.PipelinesList
}

func (r *EmptyTableResolver) Start() {
}

func (r *EmptyTableResolver) Stop() {
}
