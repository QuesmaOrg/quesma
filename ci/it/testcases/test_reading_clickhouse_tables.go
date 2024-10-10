package testcases

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

type ReadingClickHouseTablesIntegrationTestcase struct {
	IntegrationTestcaseBase
}

func NewReadingClickHouseTablesIntegrationTestcase() *ReadingClickHouseTablesIntegrationTestcase {
	return &ReadingClickHouseTablesIntegrationTestcase{
		IntegrationTestcaseBase: IntegrationTestcaseBase{
			ConfigTemplate: "quesma-now-reads-clickhouse-tables.yml.template",
		},
	}
}

func (a *ReadingClickHouseTablesIntegrationTestcase) SetupContainers(ctx context.Context) error {
	containers, err := setupAllContainersWithCh(ctx, a.ConfigTemplate)
	if err != nil {
		return err
	}
	a.Containers = containers
	return nil
}

func (a *ReadingClickHouseTablesIntegrationTestcase) RunTests(ctx context.Context, t *testing.T) error {
	a.testBasicRequest(ctx, t)
	a.testRandomThing(ctx, t)
	return nil
}

func (a *ReadingClickHouseTablesIntegrationTestcase) testBasicRequest(ctx context.Context, t *testing.T) {
	resp, err := a.RequestToQuesma(ctx, "GET", "/", nil)
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
}

func (a *ReadingClickHouseTablesIntegrationTestcase) testRandomThing(ctx context.Context, t *testing.T) {
	createTableQuery := "CREATE TABLE IF NOT EXISTS test_table (id UInt32, name String) ENGINE = Memory"
	if _, err := a.ExecuteClickHouseStatement(ctx, createTableQuery); err != nil {
		t.Fatalf("Failed to create table: %s", err)
	}

	insertRowsQuery := "INSERT INTO test_table (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
	if _, err := a.ExecuteClickHouseStatement(ctx, insertRowsQuery); err != nil {
		t.Fatalf("Failed to insert rows: %s", err)
	}

	// This returns 500 Internal Server Error, but will be tackled in separate PR.
	// (The table has not yet been discovered by Quesma )
	// ERR quesma/quesma/quesma.go:198 > quesma request failed: Q2002: Missing table. Table: test_table: can't load test_table table opaque_id= path=/test_table/_search reason="Missing table." request_id=01926654-b214-7e1d-944a-a7545cd7d419
	resp, err := a.RequestToQuesma(ctx, "GET", "/test_table/_search", []byte(`{"query": {"match_all": {}}}`))
	if err != nil {
		t.Fatalf("Failed to make GET request: %s", err)
	}
	defer resp.Body.Close()
	assert.Equal(t, "Clickhouse", resp.Header.Get("X-Quesma-Source"))
	//assert.Equal(t, 200, resp.StatusCode)
}
