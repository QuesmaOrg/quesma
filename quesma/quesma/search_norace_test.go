//go:build !race

/*
  This file contains  tests which can raise a race condition.
*/

package quesma

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/model"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/quesma/types"
	"mitmproxy/quesma/quesma/ui"
	"mitmproxy/quesma/telemetry"
	"mitmproxy/quesma/testdata"
	"mitmproxy/quesma/tracing"
	"testing"
	"time"
)

// TestAllUnsupportedQueryTypesAreProperlyRecorded tests if all unsupported query types are properly recorded.
// It runs |testdata.UnsupportedAggregationsTests| tests, each of them sends one query of unsupported type.
// It ensures that this query type is recorded in the management console, and that all other query types are not.
func TestAllUnsupportedQueryTypesAreProperlyRecorded(t *testing.T) {
	for _, tt := range testdata.UnsupportedQueriesTests {
		t.Run(tt.TestName, func(t *testing.T) {
			if tt.QueryType == "script" {
				t.Skip("Only 1 test. We can't deal with scripts inside queries yet. It fails very early, during JSON unmarshalling, so we can't even know the type of aggregation.")
			}
			db, _, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			assert.NoError(t, err)

			lm := clickhouse.NewLogManagerWithConnection(db, table)
			cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {Enabled: true}}}
			logChan := logger.InitOnlyChannelLoggerForTests(cfg, &tracing.AsyncTraceLogger{AsyncQueryTrace: concurrent.NewMap[string, tracing.TraceCtx]()})
			managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, logChan, telemetry.NewPhoneHomeEmptyAgent())
			go managementConsole.RunOnlyChannelProcessor()

			queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)
			newCtx := context.WithValue(ctx, tracing.RequestIdCtxKey, tracing.GetRequestId())
			_, _ = queryRunner.handleSearch(newCtx, tableName, types.MustJSON(tt.QueryRequestJson))

			for _, queryType := range model.AllQueryTypes {
				if queryType != tt.QueryType {
					assert.Len(t, managementConsole.QueriesWithUnsupportedType(queryType), 0)
				}
			}

			// Update of the count below is done asynchronously in another goroutine
			// (go managementConsole.RunOnlyChannelProcessor() above), so we might need to wait a bit
			assert.Eventually(t, func() bool {
				return len(managementConsole.QueriesWithUnsupportedType(tt.QueryType)) == 1
			}, 250*time.Millisecond, 1*time.Millisecond)
			assert.Equal(t, 1, managementConsole.GetTotalUnsupportedQueries())
			assert.Equal(t, 1, managementConsole.GetSavedUnsupportedQueries())
			assert.Equal(t, 1, len(managementConsole.GetUnsupportedTypesWithCount()))
		})
	}
}

// TestDifferentUnsupportedQueries tests if different unsupported queries are properly recorded.
// I randomly select requestsNr queries from testdata.UnsupportedAggregationsTests, run them, and check
// if all of them are properly recorded in the management console.
func TestDifferentUnsupportedQueries(t *testing.T) {
	const maxSavedQueriesPerQueryType = 10
	const requestsNr = 50

	// generate random |requestsNr| queries to send
	testNrs := make([]int, 0, requestsNr)
	testCounts := make([]int, len(testdata.UnsupportedQueriesTests))
	for range requestsNr {
		randInt := rand.Intn(len(testdata.UnsupportedQueriesTests))
		if testdata.UnsupportedQueriesTests[randInt].QueryType == "script" {
			// We can't deal with scripts inside queries yet. It fails very early, during JSON unmarshalling, so we can't even know the type of aggregation.
			continue
		}
		testNrs = append(testNrs, randInt)
		testCounts[randInt]++
	}

	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	assert.NoError(t, err)

	lm := clickhouse.NewLogManagerWithConnection(db, table)
	cfg := config.QuesmaConfiguration{IndexConfig: map[string]config.IndexConfiguration{tableName: {Enabled: true}}}
	logChan := logger.InitOnlyChannelLoggerForTests(cfg, &tracing.AsyncTraceLogger{AsyncQueryTrace: concurrent.NewMap[string, tracing.TraceCtx]()})
	managementConsole := ui.NewQuesmaManagementConsole(cfg, nil, nil, logChan, telemetry.NewPhoneHomeEmptyAgent())
	go managementConsole.RunOnlyChannelProcessor()

	queryRunner := NewQueryRunner(lm, cfg, nil, managementConsole)
	for _, testNr := range testNrs {
		newCtx := context.WithValue(ctx, tracing.RequestIdCtxKey, tracing.GetRequestId())
		_, _ = queryRunner.handleSearch(newCtx, tableName, types.MustJSON(testdata.UnsupportedQueriesTests[testNr].QueryRequestJson))
	}

	for i, tt := range testdata.UnsupportedQueriesTests {
		// Update of the count below is done asynchronously in another goroutine
		// (go managementConsole.RunOnlyChannelProcessor() above), so we might need to wait a bit
		assert.Eventually(t, func() bool {
			return len(managementConsole.QueriesWithUnsupportedType(tt.QueryType)) == min(testCounts[i], maxSavedQueriesPerQueryType)
		}, 600*time.Millisecond, 1*time.Millisecond,
			tt.TestName+": wanted: %d, got: %d", min(testCounts[i], maxSavedQueriesPerQueryType),
			len(managementConsole.QueriesWithUnsupportedType(tt.QueryType)),
		)
	}
}
