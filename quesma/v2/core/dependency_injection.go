// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"quesma_v2/core/diag"
)

type DiagnosticInjector interface {
	InjectDiagnostic(s diag.Diagnostic)
}

type Dependencies struct {
	Diagnostic diag.Diagnostic
}

func NewDI() *Dependencies {
	return &Dependencies{}
}

func (d *Dependencies) InjectDependencies(a any) {

	if injector, ok := a.(DiagnosticInjector); ok {
		injector.InjectDiagnostic(d.Diagnostic)
	}

}
