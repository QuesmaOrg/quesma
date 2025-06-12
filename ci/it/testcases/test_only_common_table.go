// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

type OnlyCommonTableTestcase struct {
	IntegrationTestcaseBase
}

func NewOnlyCommonTableTestcase() *OnlyCommonTableTestcase {
	return &OnlyCommonTableTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-only-common-table.yml.template",
		},
	}
}

func (a *OnlyCommonTableTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	a.Containers = containers
	return err
}

func (a *OnlyCommonTableTestcase) RunTests(ctx context.Context, t *testing.T) error {

	t.Run("test alter virtual table", func(t *testing.T) { a.testAlterVirtualTable(ctx, t) })
	return nil
}

func (a *OnlyCommonTableTestcase) testAlterVirtualTable(ctx context.Context, t *testing.T) {

	reloadTables := func() {
		resp, body := a.RequestToQuesma(ctx, t, "POST", "/_quesma/reload-tables", nil)
		fmt.Println(string(body))
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	resp, body := a.RequestToQuesma(ctx, t, "POST", "/logs-6/_doc", []byte(`{"name": "Przemyslaw", "age": 31337}`))
	fmt.Println(string(body))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	reloadTables()

	resp, body = a.RequestToQuesma(ctx, t, "POST", "/logs-6/_doc", []byte(`{"name": "Przemyslaw", "age": 31337, "this-is-a-new-field": "new-field"}`))
	fmt.Println(string(body))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	fieldCapsQuery := `{"fields": [ "*" ]}`

	_, bodyBytes := a.RequestToQuesma(ctx, t, "POST", "/logs-6/_field_caps", []byte(fieldCapsQuery))
	assert.Contains(t, string(bodyBytes), `"this-is-a-new-field"`)

	reloadTables()

	_, bodyBytes = a.RequestToQuesma(ctx, t, "POST", "/logs-6/_field_caps", []byte(fieldCapsQuery))
	fmt.Println(string(bodyBytes))
	assert.Contains(t, string(bodyBytes), `"this-is-a-new-field"`)

}
