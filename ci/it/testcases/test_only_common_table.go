// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
	"time"
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

	resp, body := a.RequestToQuesma(ctx, t, "POST", "/logs-6/_doc", []byte(`{"name": "Przemyslaw", "age": 31337}`))
	fmt.Println(string(body))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// wait for internal processing, especially for the periodic task that updates the schema
	time.Sleep(60 * time.Second)

	resp, body = a.RequestToQuesma(ctx, t, "POST", "/logs-6/_doc", []byte(`{"name": "Przemyslaw", "age": 31337, "this-is-a-new-field": "new-field"}`))
	fmt.Println(string(body))
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	q := `{	"fields": [ "*" ]}`

	_, bodyBytes := a.RequestToQuesma(ctx, t, "POST", "/logs-6/_field_caps", []byte(q))

	fmt.Println(string(bodyBytes))

	assert.Contains(t, string(bodyBytes), `"this-is-a-new-field"`)

}
