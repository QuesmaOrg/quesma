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
	if err := testCase.SetupContainers(ctx); err != nil {
		t.Fatalf("Failed to setup containers: %s", err)
	}
	if err := testCase.RunTests(ctx, t); err != nil {
		t.Fatalf("Failed to run tests: %s", err)
	}
	testCase.Cleanup(ctx)
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
