// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"fmt"
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

func EmptyDependencies() *Dependencies {
	return &Dependencies{
		Diagnostic: diag.EmptyDiagnostic(),
	}
}

const traceDependencyInjection bool = false

// InjectDependenciesInto injects dependencies into a component. This is indented to use in Quesma building process only.
func (d *Dependencies) InjectDependenciesInto(a any) {

	// TODO fmt for now. Later we can use logger. We need to move logger to the V2 module.

	if traceDependencyInjection {
		fmt.Printf("BEGIN - Injecting dependencies into %T. \n", a)
	}

	if injector, ok := a.(DiagnosticInjector); ok {
		injector.InjectDiagnostic(d.Diagnostic)
		if traceDependencyInjection {
			fmt.Printf("  OK - Injected Diagnostic into %T\n", a)
		}
	} else {
		if traceDependencyInjection {
			fmt.Printf("  SKIP - No Diagnostic to inject into %T. It doesn't implement DiagnosticInjector interface.\n", a)
		}
	}

	if traceDependencyInjection {
		fmt.Printf("END - Injecting dependencies into %T\n", a)
	}
}
