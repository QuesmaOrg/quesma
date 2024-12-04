// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import "quesma/frontend_connectors"

type TableResolver interface {
	Start()
	Stop()

	Resolve(pipeline string, indexPattern string) *frontend_connectors.Decision

	Pipelines() []string
	RecentDecisions() []frontend_connectors.PatternDecisions
}
