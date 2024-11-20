// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"quesma/concurrent"
	"quesma/end_user_errors"
	"quesma/logger"
	"quesma/persistence"
	"quesma/quesma/config"
	"quesma/quesma/recovery"

	"quesma/telemetry"
	"quesma/util"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

const (
	timestampFieldName             = "@timestamp" // it's always DateTime64 for now, don't want to waste time changing that, we don't seem to use that anyway
	allElasticsearchIndicesPattern = "_all"
)

type (
	LogManager struct {
		ctx            context.Context
		cancel         context.CancelFunc
		chDb           *sql.DB
		tableDiscovery TableDiscovery
		cfg            *config.QuesmaConfiguration
		phoneHomeAgent telemetry.PhoneHomeAgent
	}
	TableMap  = concurrent.Map[string, *Table]
	SchemaMap = map[string]interface{} // TODO remove
	Attribute struct {
		KeysArrayName   string
		ValuesArrayName string
		TypesArrayName  string
		MapValueName    string
		MapMetadataName string
		Type            BaseType
	}
	ChTableConfig struct {
		HasTimestamp bool // does table have 'timestamp' field
		// allow_suspicious_Ttl_expressions=1 to enable TTL without date field (doesn't work for me!)
		// also be very cautious with it and test it beforehand, people say it doesn't work properly
		// TODO make sure it's unique in schema (there's no other 'timestamp' field)
		// I (Krzysiek) can write it quickly, but don't want to waste time for it right now.
		TimestampDefaultsNow bool
		Engine               string // "Log", "MergeTree", etc.
		OrderBy              string // "" if none
		PartitionBy          string // "" if none
		PrimaryKey           string // "" if none
		Settings             string // "" if none
		Ttl                  string // of type Interval, e.g. 3 MONTH, 1 YEAR
		// look https://clickhouse.com/docs/en/sql-reference/data-types/special-data-types/interval
		// "" if none
		// TODO make sure it's unique in schema (there's no other 'others' field)
		// I (Krzysiek) can write it quickly, but don't want to waste time for it right now.
		Attributes                            []Attribute
		CastUnsupportedAttrValueTypesToString bool // if we have e.g. only attrs (String, String), we'll cast e.g. Date to String
		PreferCastingToOthers                 bool // we'll put non-schema field in [String, String] attrs map instead of others, if we have both options
	}
)

func NewTableMap() *TableMap {
	return concurrent.NewMap[string, *Table]()
}

func (lm *LogManager) Start() {
	if err := lm.chDb.Ping(); err != nil {
		endUserError := end_user_errors.GuessClickhouseErrorType(err)
		logger.ErrorWithCtxAndReason(lm.ctx, endUserError.Reason()).Msgf("could not connect to clickhouse. error: %v", endUserError)
	}

	lm.tableDiscovery.ReloadTableDefinitions()

	logger.Info().Msgf("schemas loaded: %s", lm.tableDiscovery.TableDefinitions().Keys())
	const reloadInterval = 1 * time.Minute
	forceReloadCh := lm.tableDiscovery.ForceReloadCh()

	go func() {
		recovery.LogPanic()
		for {
			select {
			case <-lm.ctx.Done():
				logger.Debug().Msg("closing log manager")
				return
			case doneCh := <-forceReloadCh:
				// this prevents flood of reloads, after a long pause
				if time.Since(lm.tableDiscovery.LastReloadTime()) > reloadInterval {
					lm.tableDiscovery.ReloadTableDefinitions()
				}
				doneCh <- struct{}{}
			case <-time.After(reloadInterval):
				// only reload if we actually use Quesma, make it double time to prevent edge case
				// otherwise it prevent ClickHouse Cloud from idle pausing
				if time.Since(lm.tableDiscovery.LastAccessTime()) < reloadInterval*2 {
					lm.tableDiscovery.ReloadTableDefinitions()
				}
			}
		}
	}()
}

func (lm *LogManager) Stop() {
	lm.cancel()
}

type discoveredTable struct {
	name               string
	databaseName       string
	columnTypes        map[string]columnMetadata
	config             config.IndexConfiguration
	comment            string
	createTableQuery   string
	timestampFieldName string
	virtualTable       bool
}

func (lm *LogManager) ReloadTables() {
	logger.Info().Msg("reloading tables definitions")
	lm.tableDiscovery.ReloadTableDefinitions()
}

func (lm *LogManager) Close() {
	_ = lm.chDb.Close()
}

// ResolveIndexPattern - takes incoming index pattern (e.g. "index-*" or multiple patterns like "index-*,logs-*")
// and returns all matching indexes. Empty pattern means all indexes, "_all" index name means all indexes
//
//	Note: Empty pattern means all indexes, "_all" index name means all indexes
func (lm *LogManager) ResolveIndexPattern(ctx context.Context, pattern string) (results []string, err error) {
	if err = lm.tableDiscovery.TableDefinitionsFetchError(); err != nil {
		return nil, err
	}

	results = make([]string, 0)
	if strings.Contains(pattern, ",") {
		for _, pattern := range strings.Split(pattern, ",") {
			if pattern == allElasticsearchIndicesPattern || pattern == "" {
				results = lm.tableDiscovery.TableDefinitions().Keys()
				slices.Sort(results)
				return results, nil
			} else {
				indexes, err := lm.ResolveIndexPattern(ctx, pattern)
				if err != nil {
					return nil, err
				}
				results = append(results, indexes...)
			}
		}
	} else {
		if pattern == allElasticsearchIndicesPattern || len(pattern) == 0 {
			results = lm.tableDiscovery.TableDefinitions().Keys()
			slices.Sort(results)
			return results, nil
		} else {
			lm.tableDiscovery.TableDefinitions().
				Range(func(tableName string, v *Table) bool {
					if util.IndexPatternMatches(pattern, tableName) {
						results = append(results, tableName)
					}
					return true
				})
		}
	}

	return util.Distinct(results), nil
}

func (lm *LogManager) CountMultiple(ctx context.Context, tables ...string) (int64, error) {
	if len(tables) == 0 {
		return 0, nil
	}
	const subcountStatement = "(SELECT count(*) FROM ?)"
	var subCountStatements []string
	for range len(tables) {
		subCountStatements = append(subCountStatements, subcountStatement)
	}

	var count int64
	var anyTables []any
	for _, t := range tables {
		anyTables = append(anyTables, t)
	}
	err := lm.chDb.QueryRowContext(ctx, fmt.Sprintf("SELECT sum(*) as count FROM (%s)", strings.Join(subCountStatements, " UNION ALL ")), anyTables...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: query row failed: %v", err)
	}
	return count, nil
}

func (lm *LogManager) Count(ctx context.Context, table string) (int64, error) {
	var count int64
	err := lm.chDb.QueryRowContext(ctx, "SELECT count(*) FROM ?", table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("clickhouse: query row failed: %v", err)
	}
	return count, nil
}

func (lm *LogManager) executeRawQuery(query string) (*sql.Rows, error) {
	if res, err := lm.chDb.Query(query); err != nil {
		return nil, fmt.Errorf("error in executeRawQuery: query: %s\nerr:%v", query, err)
	} else {
		return res, nil
	}
}

func (lm *LogManager) GetDB() *sql.DB {
	return lm.chDb
}

/* The logic below contains a simple checks that are executed by connectors to ensure that they are
not connected to the data sources which are not allowed by current license. */

type PaidServiceName int

const (
	CHCloudServiceName PaidServiceName = iota
	HydrolixServiceName
)

func (s PaidServiceName) String() string {
	return [...]string{"ClickHouse Cloud", "Hydrolix"}[s]
}

var paidServiceChecks = map[PaidServiceName]string{
	HydrolixServiceName: `SELECT concat(database,'.', table) FROM system.tables WHERE engine = 'TurbineStorage';`,
	CHCloudServiceName:  `SELECT concat(database,'.', table) FROM system.tables WHERE engine = 'SharedMergeTree';`,
	// For CH Cloud we can also check the output of the following query: --> `SELECT * FROM system.settings WHERE name='cloud_mode_engine';`
}

func (lm *LogManager) isConnectedToPaidService(service PaidServiceName) (bool, error) {
	rows, err := lm.executeRawQuery(paidServiceChecks[service])
	if err != nil {
		return false, fmt.Errorf("error executing %s-identifying query: %v", service, err)
	}
	defer rows.Close()
	if rows.Next() {
		return true, fmt.Errorf("detected %s-specific table engine, which is not allowed", service)
	}
	return false, nil
}

func (lm *LogManager) CheckIfConnectedPaidService(service PaidServiceName) (returnedErr error) {
	if _, ok := paidServiceChecks[service]; !ok {
		return fmt.Errorf("service %s is not supported", service)
	}
	for {
		isConnectedToPaidService, err := lm.isConnectedToPaidService(service)
		if err != nil {
			logger.Error().Msgf("Licensing checker failed to connect with the database")
		}
		if isConnectedToPaidService {
			return fmt.Errorf("detected %s-specific table engine, which is not allowed", service)
		} else if err == nil { // no paid service detected, no conn errors
			returnedErr = nil
			break
		}
		time.Sleep(3 * time.Second)
	}
	return returnedErr
}

func (lm *LogManager) FindTable(tableName string) (result *Table) {
	tableNamePattern := util.TableNamePatternRegexp(tableName)
	lm.tableDiscovery.TableDefinitions().
		Range(func(name string, table *Table) bool {
			if tableNamePattern.MatchString(name) {
				result = table
				return false
			}
			return true
		})

	return result
}

func (lm *LogManager) GetTableDefinitions() (TableMap, error) {
	if err := lm.tableDiscovery.TableDefinitionsFetchError(); err != nil {
		return *lm.tableDiscovery.TableDefinitions(), err
	}

	return *lm.tableDiscovery.TableDefinitions(), nil
}

// Returns if schema wasn't created (so it needs to be, and will be in a moment)
func (lm *LogManager) AddTableIfDoesntExist(table *Table) bool {
	t := lm.FindTable(table.Name)
	if t == nil {
		table.Created = true

		table.ApplyIndexConfig(lm.cfg)

		lm.tableDiscovery.TableDefinitions().Store(table.Name, table)
		return true
	}
	wasntCreated := !t.Created
	t.Created = true
	return wasntCreated
}

func (lm *LogManager) Ping() error {
	return lm.chDb.Ping()
}

func NewEmptyLogManager(cfg *config.QuesmaConfiguration, chDb *sql.DB, phoneHomeAgent telemetry.PhoneHomeAgent, loader TableDiscovery) *LogManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &LogManager{ctx: ctx, cancel: cancel, chDb: chDb, tableDiscovery: loader, cfg: cfg, phoneHomeAgent: phoneHomeAgent}
}

func NewLogManager(tables *TableMap, cfg *config.QuesmaConfiguration) *LogManager {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(tables)
	return &LogManager{chDb: nil, tableDiscovery: NewTableDiscoveryWith(cfg, nil, *tables),
		cfg: cfg, phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent(),
	}
}

// right now only for tests purposes
func NewLogManagerWithConnection(db *sql.DB, tables *TableMap) *LogManager {
	return &LogManager{chDb: db, tableDiscovery: NewTableDiscoveryWith(&config.QuesmaConfiguration{}, db, *tables),
		phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent()}
}

func NewLogManagerEmpty() *LogManager {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(NewTableMap())
	cfg := &config.QuesmaConfiguration{}
	return &LogManager{tableDiscovery: NewTableDiscovery(cfg, nil, persistence.NewStaticJSONDatabase()), cfg: cfg,
		phoneHomeAgent: telemetry.NewPhoneHomeEmptyAgent()}
}

func NewDefaultCHConfig() *ChTableConfig {
	return &ChTableConfig{
		HasTimestamp:         true,
		TimestampDefaultsNow: true,
		Engine:               "MergeTree",
		OrderBy:              "(" + `"@timestamp"` + ")",
		PartitionBy:          "",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []Attribute{
			NewDefaultInt64Attribute(),
			NewDefaultFloat64Attribute(),
			NewDefaultBoolAttribute(),
			NewDefaultStringAttribute(),
		},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
	}
}

func NewNoTimestampOnlyStringAttrCHConfig() *ChTableConfig {
	return &ChTableConfig{
		HasTimestamp:         false,
		TimestampDefaultsNow: false,
		Engine:               "MergeTree",
		OrderBy:              "(" + `"@timestamp"` + ")",
		PartitionBy:          "",
		PrimaryKey:           "",
		Ttl:                  "",
		Attributes: []Attribute{
			NewDefaultStringAttribute(),
		},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
	}
}

func NewChTableConfigNoAttrs() *ChTableConfig {
	return &ChTableConfig{
		HasTimestamp:                          false,
		TimestampDefaultsNow:                  false,
		Engine:                                "MergeTree",
		OrderBy:                               "(" + `"@timestamp"` + ")",
		Attributes:                            []Attribute{},
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
	}
}

func NewChTableConfigTimestampStringAttr() *ChTableConfig {
	return &ChTableConfig{
		HasTimestamp:                          true,
		TimestampDefaultsNow:                  true,
		Attributes:                            []Attribute{NewDefaultStringAttribute()},
		Engine:                                "MergeTree",
		OrderBy:                               "(" + "`@timestamp`" + ")",
		CastUnsupportedAttrValueTypesToString: true,
		PreferCastingToOthers:                 true,
	}
}

func (c *ChTableConfig) GetAttributes() []Attribute {
	return c.Attributes
}

func (l *LogManager) IsInTransparentProxyMode() bool {
	return l.cfg.TransparentProxy
}
