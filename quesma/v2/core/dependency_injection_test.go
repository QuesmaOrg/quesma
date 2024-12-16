// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"quesma_v2/core/diag"
	"testing"
)

type componentWithDependency struct {
	diag diag.Diagnostic
}

func (sc *componentWithDependency) InjectDiagnostic(d diag.Diagnostic) {
	sc.diag = d
}

type componentWithoutDependencyInjection struct {
	diag diag.Diagnostic
}

func Test_dependencyInjection(t *testing.T) {

	deps := NewDependencies()
	deps.Diagnostic = diag.EmptyDiagnostic()

	component1 := &componentWithDependency{}
	component2 := &componentWithoutDependencyInjection{}

	deps.InjectDependenciesInto(component1)
	deps.InjectDependenciesInto(component2)

	if component1.diag == nil {
		t.Errorf("Expected diagnostic to be injected")
	}

	if component2.diag != nil {
		t.Errorf("Expected diagnostic not to be injected")
	}

}
