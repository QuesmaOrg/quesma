// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package types

import "time"

// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
type TranslatedSQLQuery struct {
	Query []byte

	PerformedOptimizations []string
	QueryTransformations   []string

	Duration    time.Duration
	ExplainPlan string
}
