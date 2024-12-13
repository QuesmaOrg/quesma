// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"quesma_v2/core/diag"
	"testing"
)

type someComponent struct {
	diag diag.Diagnostic
}

func (sc *someComponent) InjectDiagnostic(d diag.Diagnostic) {
	sc.diag = d
}

func Test_dependencyInjection(t *testing.T) {

	deps := NewDI()

	diagnostic := diag.EmptyDiagnostic()
	deps.Diagnostic = diagnostic

	sc := &someComponent{}

	deps.InjectDependencies(sc)

	if sc.diag == nil {
		t.Errorf("Expected diagnostic to be injected")
	}



}
