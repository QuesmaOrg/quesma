// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package painless

import (
	painless_antlr "quesma/queryparser/painless/antlr"
)

type PainlessTransformer struct {
	painless_antlr.BasePainlessParserVisitor

	Errors []error
}

func NewPainlessTransformer() *PainlessTransformer {
	return &PainlessTransformer{}
}
