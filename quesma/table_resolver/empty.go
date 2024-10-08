// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

type EmptyIndexRegistry struct {
	Decisions          map[string]*Decision
	RecentDecisionList []PatternDecision
	PipelinesList      []string
}

func NewEmptyIndexRegistry() *EmptyIndexRegistry {
	return &EmptyIndexRegistry{
		Decisions: make(map[string]*Decision),
	}
}

func (r *EmptyIndexRegistry) Resolve(pipeline string, indexPattern string) *Decision {
	return r.Decisions[indexPattern]
}

func (r *EmptyIndexRegistry) RecentDecisions() []PatternDecision {
	return r.RecentDecisionList
}

func (r *EmptyIndexRegistry) Pipelines() []string {
	return r.PipelinesList
}

func (r *EmptyIndexRegistry) Start() {
}

func (r *EmptyIndexRegistry) Stop() {
}
