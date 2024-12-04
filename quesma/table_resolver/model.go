package table_resolver

import "quesma/frontend_connectors"

type TableResolver interface {
	Start()
	Stop()

	Resolve(pipeline string, indexPattern string) *frontend_connectors.Decision

	Pipelines() []string
	RecentDecisions() []frontend_connectors.PatternDecisions
}
