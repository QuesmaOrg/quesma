// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"quesma_v2/core/diag"
)

// Here are interfaces that are used to inject dependencies into structs.
// Component that require a dependency should implement the corresponding interface.
//

type DiagnosticInjector interface {
	InjectDiagnostic(s diag.Diagnostic)
}

// Dependencies is a struct that contains all the dependencies that can be injected during Quesma building.

type Dependencies struct {
	Diagnostic diag.Diagnostic
}

func NewDependencies() *Dependencies {
	return &Dependencies{}
}

// InjectDependenciesInto injects dependencies into a component. This is indented to use in Quesma building process only.
func (d *Dependencies) InjectDependenciesInto(a any) {

	if injector, ok := a.(DiagnosticInjector); ok {
		injector.InjectDiagnostic(d.Diagnostic)
	}

}
