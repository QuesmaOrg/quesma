// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package table_resolver

import (
	"github.com/QuesmaOrg/quesma/v2/core"
)

type TableResolver interface {
	Start()
	Stop()

	Resolve(pipeline string, indexPattern string) *quesma_api.Decision

	Pipelines() []string
	RecentDecisions() []quesma_api.PatternDecisions
}
