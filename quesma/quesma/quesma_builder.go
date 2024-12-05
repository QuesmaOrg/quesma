// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma

import (
	quesma_api "quesma_v2/core"
)

type QuesmaV2 struct {
	*quesma_api.Quesma
}

func NewQuesmaV2() *QuesmaV2 {
	return &QuesmaV2{
		Quesma: quesma_api.NewQuesma(),
	}
}

func (quesma *QuesmaV2) Build() (quesma_api.QuesmaBuilder, error) {
	return quesma.Quesma.Build()
}
