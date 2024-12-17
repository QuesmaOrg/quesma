// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"quesma_v2/core/diag"
	"testing"
)

type componentWithDependency struct {
	phoneHomeClient diag.PhoneHomeClient
}

func (sc *componentWithDependency) SetDependencies(deps Dependencies) {
	sc.phoneHomeClient = deps.PhoneHomeAgent()
}

type componentWithoutDependencyInjection struct {
	phoneHomeClient diag.PhoneHomeClient
}

func Test_dependencyInjection(t *testing.T) {

	deps := EmptyDependencies()

	component1 := &componentWithDependency{}
	component2 := &componentWithoutDependencyInjection{}

	deps.InjectDependenciesInto(component1)
	deps.InjectDependenciesInto(component2)

	if component1.phoneHomeClient == nil {
		t.Errorf("Expected diagnostic to be injected")
	}

	if component2.phoneHomeClient != nil {
		t.Errorf("Expected diagnostic not to be injected")
	}

}
