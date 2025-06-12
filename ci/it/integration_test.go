// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package it

import (
	"context"
	"quesma.com/its/testcases"
	"testing"
)

func runIntegrationTest(t *testing.T, testCase testcases.TestCase) {
	ctx := context.Background()
	t.Cleanup(func() {
		testCase.Cleanup(ctx, t)
	})
	if err := testCase.SetupContainers(ctx); err != nil {
		t.Fatalf("Failed to setup containers: %s", err)
	}
	if err := testCase.RunTests(ctx, t); err != nil {
		t.Fatalf("Failed to run tests: %s", err)
	}
}

func TestTransparentProxy(t *testing.T) {
	testCase := testcases.NewTransparentProxyIntegrationTestcase()
	runIntegrationTest(t, testCase)
}

func TestReadingClickHouseTablesIntegrationTestcase(t *testing.T) {
	testCase := testcases.NewReadingClickHouseTablesIntegrationTestcase()
	runIntegrationTest(t, testCase)
}

func TestQueryAndIngestPipelineTestcase(t *testing.T) {
	testCase := testcases.NewQueryAndIngestPipelineTestcase()
	runIntegrationTest(t, testCase)
}

func TestDualWriteAndCommonTableTestcase(t *testing.T) {
	testCase := testcases.NewDualWriteAndCommonTableTestcase()
	runIntegrationTest(t, testCase)
}

func TestWildcardDisabledTestcase(t *testing.T) {
	testCase := testcases.NewWildcardDisabledTestcase()
	runIntegrationTest(t, testCase)
}

func TestWildcardClickhouseTestcase(t *testing.T) {
	testCase := testcases.NewWildcardClickhouseTestcase()
	runIntegrationTest(t, testCase)
}

func TestIngestTestcase(t *testing.T) {
	testCase := testcases.NewIngestTestcase()
	runIntegrationTest(t, testCase)
}

func TestABTestcase(t *testing.T) {
	testCase := testcases.NewABTestcase()
	runIntegrationTest(t, testCase)
}

func TestIngestTypesTestcase(t *testing.T) {
	testCase := testcases.NewIngestTypesTestcase()
	runIntegrationTest(t, testCase)
}

func TestTableOverrideTestcase(t *testing.T) {
	testCase := testcases.NewOverrideTestcase()
	runIntegrationTest(t, testCase)
}

func TestIndexNameRewrite(t *testing.T) {
	testCase := testcases.NewIndexNameRewriteTestcase()
	runIntegrationTest(t, testCase)
}

func TestSplitTimeRange(t *testing.T) {
	testCase := testcases.NewSplitTimeRangeTestcase()
	runIntegrationTest(t, testCase)
}

func TestOnlyCommonTable(t *testing.T) {
	testCase := testcases.NewOnlyCommonTableTestcase()
	runIntegrationTest(t, testCase)
}
