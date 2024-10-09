// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

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
	return r.Decisions[indexPattern]
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
