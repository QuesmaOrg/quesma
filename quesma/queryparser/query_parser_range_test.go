package queryparser

import (
	"context"
	"github.com/stretchr/testify/assert"
	"mitmproxy/quesma/clickhouse"
	"mitmproxy/quesma/concurrent"
	"mitmproxy/quesma/quesma/config"
	"strings"
	"testing"
)

// Test_parseRange tests if DateTime64 field properly uses Clickhouse's 'parseDateTime64BestEffort' function
func Test_parseRange_DateTime64(t *testing.T) {
	rangePartOfQuery := QueryMap{
		"timestamp": QueryMap{
			"format": "strict_date_optional_time",
			"gte":    "2024-02-02T13:47:16.029Z",
			"lte":    "2024-02-09T13:47:16.029Z",
		},
	}
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "timestamp" DateTime64(3, 'UTC') )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background()}

	whereClause := cw.parseRange(rangePartOfQuery).Sql.Stmt
	split := strings.Split(whereClause, "parseDateTime64BestEffort")
	assert.Len(t, split, 3)
}

// Test_parseRange tests if DateTime field properly uses Clickhouse's 'parseDateTimeBestEffort' function
func Test_parseRange_DateTime(t *testing.T) {
	rangePartOfQuery := QueryMap{
		"timestamp": QueryMap{
			"format": "strict_date_optional_time",
			"gte":    "2024-02-02T13:47:16.029Z",
			"lte":    "2024-02-09T13:47:16.029Z",
		},
	}
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "timestamp" DateTime )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background()}

	whereClause := cw.parseRange(rangePartOfQuery).Sql.Stmt
	split := strings.Split(whereClause, "parseDateTimeBestEffort")
	assert.Len(t, split, 3)
}

func Test_parseRange_numeric(t *testing.T) {
	rangePartOfQuery := QueryMap{
		"time_taken": QueryMap{
			"gt": "100",
		},
	}
	table, err := clickhouse.NewTable(`CREATE TABLE `+tableName+`
		( "message" String, "timestamp" DateTime, "time_taken" UInt32 )
		ENGINE = Memory`,
		clickhouse.NewNoTimestampOnlyStringAttrCHConfig(),
	)
	if err != nil {
		t.Fatal(err)
	}
	lm := clickhouse.NewLogManager(concurrent.NewMapWith(tableName, table), config.QuesmaConfiguration{})
	cw := ClickhouseQueryTranslator{ClickhouseLM: lm, Table: table, Ctx: context.Background()}

	whereClause := cw.parseRange(rangePartOfQuery).Sql.Stmt
	assert.Equal(t, "\"time_taken\">100", whereClause)
}
