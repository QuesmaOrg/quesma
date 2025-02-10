// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package testcases

import (
	"context"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

type OverrideTestcase struct {
	IntegrationTestcaseBase
}

func NewOverrideTestcase() *OverrideTestcase {
	return &OverrideTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-table-override.yml.template",
		},
	}
}

func (a *OverrideTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	a.Containers = containers
	return err
}

func (a *OverrideTestcase) RunTests(ctx context.Context, t *testing.T) error {
	t.Run("test kibana sample flights ingest to clickhouse", func(t *testing.T) { a.testKibanaSampleFlightsIngestToClickHouse(ctx, t) })
	t.Run("test kibana sample flights search", func(t *testing.T) { a.testKibanaSampleFlightsSearch(ctx, t) })
	return nil
}

func (a *OverrideTestcase) testKibanaSampleFlightsIngestToClickHouse(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/kibana_sample_data_flights/_doc", sampleDocKibanaSampleFlights)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cols, err := a.FetchClickHouseColumns(ctx, "kibana_sample_data_flights_ext")
	assert.NoError(t, err, "error fetching clickhouse columns")
	assert.Equal(t, expectedColsKibanaSampleFlights, cols)
}

func (a *OverrideTestcase) testKibanaSampleFlightsSearch(ctx context.Context, t *testing.T) {
	resp, _ := a.RequestToQuesma(ctx, t, "POST", "/kibana_sample_data_flights/_search", []byte(`{"query": {"match_all": {}}}`))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
}
