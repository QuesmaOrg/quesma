// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"quesma/end_user_errors"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/schema"
	"quesma/util"
	"strings"
	"sync/atomic"
	"time"
)

type TableDiscovery interface {
	ReloadTableDefinitions()
	TableDefinitions() *TableMap
	TableDefinitionsFetchError() error

	LastAccessTime() time.Time
	LastReloadTime() time.Time
	ForceReloadCh() <-chan chan<- struct{}
}

type tableDiscovery struct {
	cfg                               config.QuesmaConfiguration
	tableVerifier                     tableVerifier
	SchemaManagement                  *SchemaManagement
	tableDefinitions                  *atomic.Pointer[TableMap]
	tableDefinitionsAccessUnixSec     atomic.Int64
	tableDefinitionsLastReloadUnixSec atomic.Int64
	forceReloadCh                     chan chan<- struct{}
	ReloadTablesError                 error
}

func NewTableDiscovery(cfg config.QuesmaConfiguration, schemaManagement *SchemaManagement) TableDiscovery {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(NewTableMap())
	result := &tableDiscovery{
		cfg:              cfg,
		SchemaManagement: schemaManagement,
		tableDefinitions: &tableDefinitions,
		forceReloadCh:    make(chan chan<- struct{}),
	}
	result.tableDefinitionsLastReloadUnixSec.Store(time.Now().Unix())
	return result
}

type TableDiscoveryTableProviderAdapter struct {
	TableDiscovery
}

func (t TableDiscoveryTableProviderAdapter) TableDefinitions() map[string]schema.Table {
	tableMap := t.TableDiscovery.TableDefinitions()
	tables := make(map[string]schema.Table)
	tableMap.Range(func(tableName string, value *Table) bool {
		table := schema.Table{Columns: make(map[string]schema.Column)}
		for _, column := range value.Cols {
			table.Columns[column.Name] = schema.Column{
				Name: column.Name,
				Type: column.Type.String(),
			}
		}
		tables[tableName] = table
		return true
	})
	return tables
}

func newTableDiscoveryWith(cfg config.QuesmaConfiguration, schemaManagement *SchemaManagement, tables TableMap) TableDiscovery {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(&tables)
	result := &tableDiscovery{
		cfg:              cfg,
		SchemaManagement: schemaManagement,
		tableDefinitions: &tableDefinitions,
		forceReloadCh:    make(chan chan<- struct{}),
	}
	result.tableDefinitionsLastReloadUnixSec.Store(time.Now().Unix())
	return result
}

func (sl *tableDiscovery) TableDefinitionsFetchError() error {
	return sl.ReloadTablesError
}

func (sl *tableDiscovery) TableAutodiscoveryEnabled() bool {
	return sl.cfg.IndexConfig == nil
}

func (sl *tableDiscovery) LastAccessTime() time.Time {
	timeMs := sl.tableDefinitionsAccessUnixSec.Load()
	return time.Unix(timeMs, 0)
}

func (sl *tableDiscovery) LastReloadTime() time.Time {
	timeMs := sl.tableDefinitionsLastReloadUnixSec.Load()
	return time.Unix(timeMs, 0)
}

func (sl *tableDiscovery) ForceReloadCh() <-chan chan<- struct{} {
	return sl.forceReloadCh
}

var StitchedTable map[string][]string

func (sl *tableDiscovery) TODOStitch(configuredTables map[string]discoveredTable) {

	StitchedTable = make(map[string][]string)

	{
		// FIXME this stitching setup
		allLogsTable := discoveredTable{name: "all_logs", columnTypes: make(map[string]string), config: config.IndexConfiguration{}}
		tables := []string{"kibana_sample_data_ecommerce", "kibana_sample_data_logs", "kibana_sample_data_flights"}

		StitchedTable["all_logs"] = tables

		for _, tableName := range tables {
			if _, ok := configuredTables[tableName]; !ok {
				logger.Warn().Msgf("table %s not found in the database", tableName)
				continue
			}
			for col, colType := range configuredTables[tableName].columnTypes {
				allLogsTable.columnTypes[fmt.Sprintf("%s", col)] = colType
			}
		}
		configuredTables[allLogsTable.name] = allLogsTable
	}

	{
		allLogsTable := discoveredTable{name: "oztam_audience_device_index", columnTypes: make(map[string]string), config: config.IndexConfiguration{}}
		tables := []string{"oztam_part_1", "oztam_part_2", "oztam_part_2"}

		StitchedTable[allLogsTable.name] = tables

		for _, tableName := range tables {
			if _, ok := configuredTables[tableName]; !ok {
				logger.Warn().Msgf("table %s not found in the database", tableName)
				continue
			}
			for col, colType := range configuredTables[tableName].columnTypes {
				allLogsTable.columnTypes[fmt.Sprintf("%s", col)] = colType
			}
		}
		configuredTables[allLogsTable.name] = allLogsTable
	}

}

func (sl *tableDiscovery) ReloadTableDefinitions() {
	sl.tableDefinitionsLastReloadUnixSec.Store(time.Now().Unix())
	logger.Debug().Msg("reloading tables definitions")
	var configuredTables map[string]discoveredTable
	databaseName := "default"
	if sl.cfg.ClickHouse.Database != "" {
		databaseName = sl.cfg.ClickHouse.Database
	}
	if tables, err := sl.SchemaManagement.readTables(databaseName); err != nil {
		var endUserError *end_user_errors.EndUserError
		if errors.As(err, &endUserError) {
			logger.ErrorWithCtxAndReason(context.Background(), endUserError.Reason()).Msgf("could not describe tables: %v", err)
		} else {
			logger.Error().Msgf("could not describe tables: %v", err)
		}
		sl.ReloadTablesError = err
		sl.tableDefinitions.Store(NewTableMap())
		sl.tableDefinitionsLastReloadUnixSec.Store(time.Now().Unix())
		return
	} else {
		if sl.TableAutodiscoveryEnabled() {
			configuredTables = sl.autoConfigureTables(tables, databaseName)
		} else {
			configuredTables = sl.configureTables(tables, databaseName)
		}
	}
	sl.TODOStitch(configuredTables)

	sl.ReloadTablesError = nil
	sl.verify(configuredTables)
	sl.populateTableDefinitions(configuredTables, databaseName, sl.cfg)
}

// configureTables confronts the tables discovered in the database with the configuration provided by the user, returning final list of tables managed by Quesma
func (sl *tableDiscovery) configureTables(tables map[string]map[string]string, databaseName string) (configuredTables map[string]discoveredTable) {
	configuredTables = make(map[string]discoveredTable)
	var explicitlyDisabledTables, notConfiguredTables []string
	for table, columns := range tables {
		if indexConfig, found := sl.cfg.IndexConfig[table]; found {
			if indexConfig.Enabled {
				for colName := range columns {
					if _, exists := indexConfig.Aliases[colName]; exists {
						logger.Error().Msgf("column [%s] clashes with an existing alias, table [%s]", colName, table)
					}
				}
				comment := sl.SchemaManagement.tableComment(databaseName, table)
				createTableQuery := sl.SchemaManagement.createTableQuery(databaseName, table)
				configuredTables[table] = discoveredTable{table, columns, indexConfig, comment, createTableQuery}
			} else {
				explicitlyDisabledTables = append(explicitlyDisabledTables, table)
			}
		} else {
			notConfiguredTables = append(notConfiguredTables, table)
		}
	}
	logger.Info().Msgf(
		"Table discovery results: configured=[%s], found but not configured=[%s], explicitly disabled=[%s]",
		strings.Join(util.MapKeys(configuredTables), ","),
		strings.Join(notConfiguredTables, ","),
		strings.Join(explicitlyDisabledTables, ","),
	)
	return
}

// autoConfigureTables takes the list of discovered tables and automatically configures them, returning the final list of tables managed by Quesma
func (sl *tableDiscovery) autoConfigureTables(tables map[string]map[string]string, databaseName string) (configuredTables map[string]discoveredTable) {
	configuredTables = make(map[string]discoveredTable)
	var autoDiscoResults strings.Builder
	logger.Info().Msg("Index configuration empty, running table auto-discovery")
	for table, columns := range tables {
		comment := sl.SchemaManagement.tableComment(databaseName, table)
		createTableQuery := sl.SchemaManagement.createTableQuery(databaseName, table)
		var maybeTimestampField string
		if sl.cfg.Hydrolix.IsNonEmpty() {
			maybeTimestampField = sl.SchemaManagement.tableTimestampField(databaseName, table, Hydrolix)
		} else {
			maybeTimestampField = sl.SchemaManagement.tableTimestampField(databaseName, table, ClickHouse)
		}
		if maybeTimestampField != "" {
			configuredTables[table] = discoveredTable{table, columns, config.IndexConfiguration{TimestampField: &maybeTimestampField}, comment, createTableQuery}
		} else {
			configuredTables[table] = discoveredTable{table, columns, config.IndexConfiguration{}, comment, createTableQuery}
		}
	}
	for tableName, conf := range configuredTables {
		autoDiscoResults.WriteString(fmt.Sprintf("{table: %s, timestampField: %s}, ", tableName, conf.config.GetTimestampField()))
	}
	logger.Info().Msgf("Table auto-discovery results -> %d tables found: [%s]", len(configuredTables), strings.TrimSuffix(autoDiscoResults.String(), ", "))
	return
}

func (sl *tableDiscovery) populateTableDefinitions(configuredTables map[string]discoveredTable, databaseName string, cfg config.QuesmaConfiguration) {
	tableMap := NewTableMap()
	for tableName, resTable := range configuredTables {
		var columnsMap = make(map[string]*Column)
		partiallyResolved := false
		for col, colType := range resTable.columnTypes {

			if _, isIgnored := resTable.config.IgnoredFields[col]; isIgnored {
				logger.Warn().Msgf("table %s, column %s is ignored", tableName, col)
				continue
			}
			if col != AttributesKeyColumn && col != AttributesValueColumn {
				column := resolveColumn(col, colType)
				if column != nil {
					columnsMap[col] = column
				} else {
					logger.Warn().Msgf("column '%s.%s' type: '%s' not resolved. table will be skipped", tableName, col, colType)
					partiallyResolved = true
				}
			}
		}

		if !partiallyResolved {
			table := Table{
				Created:      true,
				Name:         tableName,
				Comment:      resTable.comment,
				DatabaseName: databaseName,
				Cols:         columnsMap,
				Config: &ChTableConfig{
					attributes:                            []Attribute{},
					castUnsupportedAttrValueTypesToString: true,
					preferCastingToOthers:                 true,
				},
				CreateTableQuery: resTable.createTableQuery,
			}
			if containsAttributes(resTable.columnTypes) {
				table.Config.attributes = []Attribute{NewDefaultStringAttribute()}
			}

			table.applyIndexConfig(cfg)
			tableMap.Store(tableName, &table)

			logger.Debug().Msgf("schema for table [%s] loaded", tableName)
		} else {
			logger.Warn().Msgf("table %s not fully resolved, skipping", tableName)
		}
	}

	existing := sl.tableDefinitions.Load()
	existing.Range(func(key string, _ *Table) bool {
		if !tableMap.Has(key) {
			logger.Info().Msgf("table %s is no longer found in the database, ignoring", key)
		}
		return true
	})
	discoveredTables := make([]string, 0)
	tableMap.Range(func(key string, _ *Table) bool {
		if !existing.Has(key) {
			discoveredTables = append(discoveredTables, key)
		}
		return true
	})
	if len(discoveredTables) > 0 {
		logger.Info().Msgf("discovered new tables: %s", discoveredTables)
	}
	sl.tableDefinitions.Store(tableMap)
}

func (sl *tableDiscovery) TableDefinitions() *TableMap {
	sl.tableDefinitionsAccessUnixSec.Store(time.Now().Unix())
	lastReloadUnixSec := sl.tableDefinitionsLastReloadUnixSec.Load()
	lastReload := time.Unix(lastReloadUnixSec, 0)
	if time.Since(lastReload) > 15*time.Minute { // maybe configure
		logger.Info().Msg("Table definitions are stale for 15 minutes, forcing reload")
		doneCh := make(chan struct{}, 1)
		sl.forceReloadCh <- doneCh
		<-doneCh
	}
	return sl.tableDefinitions.Load()
}

func (sl *tableDiscovery) verify(tables map[string]discoveredTable) {
	for _, table := range tables {
		logger.Info().Msgf("verifying table %s", table.name)
		if correct, violations := sl.tableVerifier.verify(table); correct {
			logger.Debug().Msgf("table %s verified", table.name)
		} else {
			logger.Warn().Msgf("table %s verification failed: %s", table.name, violations)
		}
	}
}

func resolveColumn(colName, colType string) *Column {
	isNullable := false
	if isNullableType(colType) {
		isNullable = true
		colType = strings.TrimSuffix(strings.TrimPrefix(colType, "Nullable("), ")")
	}

	if isArrayType(colType) {
		arrayType := strings.TrimSuffix(strings.TrimPrefix(colType, "Array("), ")")
		if isNullableType(arrayType) {
			isNullable = true
			arrayType = strings.TrimSuffix(strings.TrimPrefix(arrayType, "Nullable("), ")")
		}
		goType := ResolveType(arrayType)
		if goType != nil {
			return &Column{
				Name: colName,
				Type: CompoundType{
					Name:     "Array",
					BaseType: BaseType{Name: arrayType, goType: goType, Nullable: isNullable},
				},
			}
		} else if isTupleType(arrayType) {
			tupleColumn := resolveColumn("Tuple", arrayType)
			if tupleColumn == nil {
				logger.Warn().Msgf("invalid tuple type for column %s, %s", colName, colType)
				return nil
			}
			tupleTyp, ok := tupleColumn.Type.(MultiValueType)
			if !ok {
				logger.Warn().Msgf("invalid tuple type for column %s, %s", colName, colType)
				return nil
			}
			return &Column{
				Name: colName,
				Type: CompoundType{
					Name:     "Array",
					BaseType: tupleTyp,
				},
			}
		} else {
			return nil
		}
	} else if isTupleType(colType) {
		indexAfterMatch, columns := parseMultiValueType(colType, len("Tuple"))
		if indexAfterMatch == -1 {
			logger.Warn().Msgf("failed parsing tuple type for column %s, %s", colName, colType)
			return nil
		}
		return &Column{
			Name: colName,
			Type: MultiValueType{
				Name: "Tuple",
				Cols: columns,
			},
		}
	} else if isEnumType(colType) {
		// TODO proper support for enums
		// For now we use Int32
		return &Column{
			Name: colName,
			Type: BaseType{
				Name:   "Int32",
				goType: NewBaseType("Int32").goType,
			},
		}
	}

	// It's not array or tuple -> it's base type
	if strings.HasPrefix(colType, "DateTime") {
		colType = removePrecision(colType)
	}
	if goType := ResolveType(colType); goType != nil {
		return &Column{
			Name: colName,
			Type: BaseType{
				Name:     colType,
				goType:   NewBaseType(colType).goType,
				Nullable: isNullable,
			},
		}
	} else {
		logger.Warn().Msgf("unknown type for column %s, %s", colName, colType)
		typeName := "Unknown(" + colType + ")"
		return &Column{
			Name: colName,
			Type: BaseType{
				Name:     typeName,
				goType:   NewBaseType("Unknown").goType,
				Nullable: isNullable,
			},
		}
	}
}

func isArrayType(colType string) bool {
	return strings.HasPrefix(colType, "Array(") && strings.HasSuffix(colType, ")")
}

func isTupleType(colType string) bool {
	return strings.HasPrefix(colType, "Tuple(") && strings.HasSuffix(colType, ")")
}

func isEnumType(colType string) bool {
	return strings.HasPrefix(colType, "Enum")
}

func isNullableType(colType string) bool {
	return strings.HasPrefix(colType, "Nullable(")
}

func containsAttributes(cols map[string]string) bool {
	hasAttributesKey := false
	hasAttributesValues := false
	for col, colType := range cols {
		if col == AttributesKeyColumn && colType == attributesColumnType {
			hasAttributesKey = true
		}
		if col == AttributesValueColumn && colType == attributesColumnType {
			hasAttributesValues = true
		}
	}
	return hasAttributesKey && hasAttributesValues
}

func removePrecision(str string) string {
	if lastIndex := strings.LastIndex(str, "("); lastIndex != -1 {
		return str[:lastIndex]
	} else {
		return str
	}
}
