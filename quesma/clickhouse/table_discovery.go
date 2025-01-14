// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/common_table"
	"github.com/QuesmaOrg/quesma/quesma/end_user_errors"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/persistence"
	"github.com/QuesmaOrg/quesma/quesma/quesma/config"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/util"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
	"github.com/goccy/go-json"
	"strings"
	"sync/atomic"
	"time"
)

type DbKind int

const (
	ClickHouse DbKind = iota //"clickhouse"
	Hydrolix                 // = "hydrolix"
)

func (d DbKind) String() string {
	return [...]string{"clickhouse", "hydrolix"}[d]
}

type TableDiscovery interface {
	ReloadTableDefinitions()
	TableDefinitions() *TableMap
	TableDefinitionsFetchError() error

	LastAccessTime() time.Time
	LastReloadTime() time.Time
	ForceReloadCh() <-chan chan<- struct{}
	AutodiscoveryEnabled() bool
}

type tableDiscovery struct {
	cfg                               *config.QuesmaConfiguration
	dbConnPool                        quesma_api.BackendConnector
	tableDefinitions                  *atomic.Pointer[TableMap]
	tableDefinitionsAccessUnixSec     atomic.Int64
	tableDefinitionsLastReloadUnixSec atomic.Int64
	forceReloadCh                     chan chan<- struct{}
	ReloadTablesError                 error
	virtualTableStorage               persistence.JSONDatabase
}

type columnMetadata struct {
	colType string
	// currently column contains original field value
	// we use it as persistent storage and load it
	// in the case when we don't control ingest
	comment string
}

func NewTableDiscovery(cfg *config.QuesmaConfiguration, dbConnPool quesma_api.BackendConnector, virtualTablesDB persistence.JSONDatabase) TableDiscovery {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(NewTableMap())
	result := &tableDiscovery{
		cfg:                 cfg,
		dbConnPool:          dbConnPool,
		tableDefinitions:    &tableDefinitions,
		forceReloadCh:       make(chan chan<- struct{}),
		virtualTableStorage: virtualTablesDB,
	}
	result.tableDefinitionsLastReloadUnixSec.Store(time.Now().Unix())
	return result
}

type TableDiscoveryTableProviderAdapter struct {
	TableDiscovery
}

func (t TableDiscoveryTableProviderAdapter) TableDefinitions() map[string]schema.Table {

	// here we filter out our internal columns

	internalColumn := make(map[string]bool)
	internalColumn[AttributesValuesColumn] = true
	internalColumn[AttributesMetadataColumn] = true
	internalColumn[DeprecatedAttributesKeyColumn] = true
	internalColumn[DeprecatedAttributesValueColumn] = true
	internalColumn[DeprecatedAttributesValueType] = true

	tableMap := t.TableDiscovery.TableDefinitions()
	tables := make(map[string]schema.Table)
	tableMap.Range(func(tableName string, value *Table) bool {
		table := schema.Table{Columns: make(map[string]schema.Column)}
		for _, column := range value.Cols {
			if internalColumn[column.Name] {
				continue
			}

			table.Columns[column.Name] = schema.Column{
				Name:    column.Name,
				Type:    column.Type.String(),
				Comment: column.Comment,
			}
		}
		table.DatabaseName = value.DatabaseName
		tables[tableName] = table
		return true
	})
	return tables
}

func NewTableDiscoveryWith(cfg *config.QuesmaConfiguration, dbConnPool quesma_api.BackendConnector, tables TableMap) TableDiscovery {
	var tableDefinitions = atomic.Pointer[TableMap]{}
	tableDefinitions.Store(&tables)
	result := &tableDiscovery{
		cfg:              cfg,
		dbConnPool:       dbConnPool,
		tableDefinitions: &tableDefinitions,
		forceReloadCh:    make(chan chan<- struct{}),
	}
	result.tableDefinitionsLastReloadUnixSec.Store(time.Now().Unix())
	return result
}

func (td *tableDiscovery) TableDefinitionsFetchError() error {
	return td.ReloadTablesError
}

func (td *tableDiscovery) AutodiscoveryEnabled() bool {
	return td.cfg.IndexAutodiscoveryEnabled()
}

func (td *tableDiscovery) LastAccessTime() time.Time {
	timeMs := td.tableDefinitionsAccessUnixSec.Load()
	return time.Unix(timeMs, 0)
}

func (td *tableDiscovery) LastReloadTime() time.Time {
	timeMs := td.tableDefinitionsLastReloadUnixSec.Load()
	return time.Unix(timeMs, 0)
}

func (td *tableDiscovery) ForceReloadCh() <-chan chan<- struct{} {
	return td.forceReloadCh
}

func (td *tableDiscovery) ReloadTableDefinitions() {
	td.tableDefinitionsLastReloadUnixSec.Store(time.Now().Unix())
	logger.Debug().Msg("reloading tables definitions")
	var configuredTables map[string]discoveredTable
	databaseName := "default"
	if td.cfg.ClickHouse.Database != "" {
		databaseName = td.cfg.ClickHouse.Database
	}
	// TODO here we should read table definition from the elastic as well.
	if tables, err := td.readTables(databaseName); err != nil {
		var endUserError *end_user_errors.EndUserError
		if errors.As(err, &endUserError) {
			logger.ErrorWithCtxAndReason(context.Background(), endUserError.Reason()).Msgf("could not describe tables: %v", err)
		} else {
			logger.Error().Msgf("could not describe tables: %v", err)
		}
		td.ReloadTablesError = err
		td.tableDefinitions.Store(NewTableMap())
		td.tableDefinitionsLastReloadUnixSec.Store(time.Now().Unix())
		return
	} else {
		if td.AutodiscoveryEnabled() {
			configuredTables = td.autoConfigureTables(tables, databaseName)
		} else {
			configuredTables = td.configureTables(tables, databaseName)
		}
	}
	configuredTables = td.readVirtualTables(configuredTables)

	td.ReloadTablesError = nil
	td.populateTableDefinitions(configuredTables, databaseName, td.cfg)
}

func (td *tableDiscovery) readVirtualTables(configuredTables map[string]discoveredTable) map[string]discoveredTable {
	quesmaCommonTable, ok := configuredTables[common_table.TableName]
	if !ok {
		logger.Warn().Msg("common table not found")
		return configuredTables
	}

	virtualTables, err := td.virtualTableStorage.List()
	if err != nil {
		logger.Error().Msgf("could not list virtual tables: %v", err)
		return configuredTables
	}

	for _, virtualTable := range virtualTables {
		data, ok, err := td.virtualTableStorage.Get(virtualTable)
		if err != nil {
			logger.Error().Msgf("could not read virtual table %s: %v", virtualTable, err)
			continue
		}
		if !ok {
			logger.Warn().Msgf("virtual table %s not found", virtualTable)
			continue
		}

		var readVirtualTable common_table.VirtualTable
		err = json.Unmarshal([]byte(data), &readVirtualTable)
		if err != nil {
			logger.Error().Msgf("could not unmarshal virtual table %s: %v", virtualTable, err)
		}

		if readVirtualTable.Version != common_table.VirtualTableStructVersion {
			// migration is not supported yet
			// we simply skip the table
			logger.Warn().Msgf("skipping virtual table %s, version mismatch, actual '%s',  expecting '%s'", virtualTable, readVirtualTable.Version, common_table.VirtualTableStructVersion)
			continue
		}

		discoTable := discoveredTable{
			name:        virtualTable,
			columnTypes: make(map[string]columnMetadata),
		}

		for _, col := range readVirtualTable.Columns {

			// here we construct virtual table columns based on common table columns
			commonTableColumn, ok := quesmaCommonTable.columnTypes[col.Name]

			if ok {
				discoTable.columnTypes[col.Name] = columnMetadata{colType: commonTableColumn.colType, comment: commonTableColumn.comment}
			} else {
				logger.Warn().Msgf("column %s not found in common table but exists in virtual table %s", col.Name, virtualTable)
			}
		}

		discoTable.comment = "Virtual table. Version: " + readVirtualTable.StoredAt
		discoTable.createTableQuery = "n/a"
		discoTable.config = config.IndexConfiguration{}
		discoTable.virtualTable = true

		configuredTables[virtualTable] = discoTable
	}
	return configuredTables
}

// configureTables confronts the tables discovered in the database with the configuration provided by the user, returning final list of tables managed by Quesma
func (td *tableDiscovery) configureTables(tables map[string]map[string]columnMetadata, databaseName string) (configuredTables map[string]discoveredTable) {
	configuredTables = make(map[string]discoveredTable)
	var explicitlyDisabledTables, notConfiguredTables []string
	for table, columns := range tables {

		// single logs table is our internal table, user shouldn't configure it at all
		// and we should always include it in the list of tables managed by Quesma
		isCommonTable := table == common_table.TableName

		if indexConfig, found := td.cfg.IndexConfig[table]; found || isCommonTable {

			if isCommonTable {
				indexConfig = config.IndexConfiguration{}
			}

			if !isCommonTable && !indexConfig.IsClickhouseQueryEnabled() && !indexConfig.IsClickhouseIngestEnabled() {
				explicitlyDisabledTables = append(explicitlyDisabledTables, table)
			} else {
				comment := td.tableComment(databaseName, table)
				createTableQuery := td.createTableQuery(databaseName, table)
				// we assume here that @timestamp field is always present in the table, or it's explicitly configured
				configuredTables[table] = discoveredTable{table, databaseName, columns, indexConfig, comment, createTableQuery, "", false}
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
func (td *tableDiscovery) autoConfigureTables(tables map[string]map[string]columnMetadata, databaseName string) (configuredTables map[string]discoveredTable) {
	configuredTables = make(map[string]discoveredTable)
	var autoDiscoResults strings.Builder
	logger.Info().Msg("Index configuration empty, running table auto-discovery")
	for table, columns := range tables {
		comment := td.tableComment(databaseName, table)
		createTableQuery := td.createTableQuery(databaseName, table)
		var maybeTimestampField string
		if td.cfg.Hydrolix.IsNonEmpty() {
			maybeTimestampField = td.tableTimestampField(databaseName, table, Hydrolix)
		} else {
			maybeTimestampField = td.tableTimestampField(databaseName, table, ClickHouse)
		}
		const isVirtualTable = false
		configuredTables[table] = discoveredTable{table, databaseName, columns, config.IndexConfiguration{}, comment, createTableQuery, maybeTimestampField, isVirtualTable}

	}
	for tableName, table := range configuredTables {
		autoDiscoResults.WriteString(fmt.Sprintf("{table: %s, timestampField: %s}, ", tableName, table.timestampFieldName))
	}
	logger.Info().Msgf("Table auto-discovery results -> %d tables found: [%s]", len(configuredTables), strings.TrimSuffix(autoDiscoResults.String(), ", "))
	return
}

func (td *tableDiscovery) populateTableDefinitions(configuredTables map[string]discoveredTable, databaseName string, cfg *config.QuesmaConfiguration) {

	tableMap := NewTableMap()
	for tableName, resTable := range configuredTables {
		var columnsMap = make(map[string]*Column)
		partiallyResolved := false
		for col, columnMeta := range resTable.columnTypes {
			if resTable.config.SchemaOverrides != nil {
				if schemaOverride, found := resTable.config.SchemaOverrides.Fields[config.FieldName(col)]; found && schemaOverride.Ignored {
					logger.Warn().Msgf("table %s, column %s is ignored", tableName, col)
					continue
				}
			}

			column := resolveColumn(col, columnMeta.colType)
			if column != nil {
				column.Comment = columnMeta.comment
				columnsMap[col] = column
			} else {
				logger.Warn().Msgf("column '%s.%s' type: '%s' not resolved. table will be skipped", tableName, col, columnMeta.colType)
				partiallyResolved = true
			}

		}

		var timestampFieldName *string
		if resTable.timestampFieldName != "" {
			timestampFieldName = &resTable.timestampFieldName
		}

		if !partiallyResolved {
			table := Table{
				Created:      true,
				Name:         tableName,
				Comment:      resTable.comment,
				DatabaseName: databaseName,
				Cols:         columnsMap,
				Config: &ChTableConfig{
					Attributes:                            []Attribute{},
					CastUnsupportedAttrValueTypesToString: true,
					PreferCastingToOthers:                 true,
				},
				CreateTableQuery:             resTable.createTableQuery,
				DiscoveredTimestampFieldName: timestampFieldName,
				VirtualTable:                 resTable.virtualTable,
			}
			if containsAttributes(resTable.columnTypes) {
				table.Config.Attributes = []Attribute{NewDefaultStringAttribute()}
			}

			table.ApplyIndexConfig(cfg)
			tableMap.Store(tableName, &table)

			logger.Debug().Msgf("schema for table [%s] loaded", tableName)
		} else {
			logger.Warn().Msgf("table %s not fully resolved, skipping", tableName)
		}
	}

	existing := td.tableDefinitions.Load()
	existing.Range(func(key string, table *Table) bool {
		if table.VirtualTable {
			return true
		}
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
	td.tableDefinitions.Store(tableMap)
}

func (td *tableDiscovery) TableDefinitions() *TableMap {
	td.tableDefinitionsAccessUnixSec.Store(time.Now().Unix())
	lastReloadUnixSec := td.tableDefinitionsLastReloadUnixSec.Load()
	lastReload := time.Unix(lastReloadUnixSec, 0)
	if time.Since(lastReload) > 15*time.Minute { // maybe configure
		logger.Info().Msg("Table definitions are stale for 15 minutes, forcing reload")
		doneCh := make(chan struct{}, 1)
		td.forceReloadCh <- doneCh
		<-doneCh
	}
	return td.tableDefinitions.Load()
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
		GoType := ResolveType(arrayType)
		if GoType != nil {
			return &Column{
				Name: colName,
				Type: CompoundType{
					Name:     "Array",
					BaseType: BaseType{Name: arrayType, GoType: GoType, Nullable: isNullable},
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
				GoType: NewBaseType("Int32").GoType,
			},
		}
	}

	// It's not array or tuple -> it's base type
	if strings.HasPrefix(colType, "DateTime") {
		colType = removePrecision(colType)
	}
	if GoType := ResolveType(colType); GoType != nil {
		return &Column{
			Name: colName,
			Type: BaseType{
				Name:     colType,
				GoType:   NewBaseType(colType).GoType,
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
				GoType:   NewBaseType("Unknown").GoType,
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

func containsAttributes(cols map[string]columnMetadata) bool {
	hasAttributesValuesColumn := false
	hasAttributesMetadataColumn := false
	for col, columnMeta := range cols {
		if col == AttributesValuesColumn && columnMeta.colType == attributesColumnType {
			hasAttributesValuesColumn = true
		}
		if col == AttributesMetadataColumn && columnMeta.colType == attributesColumnType {
			hasAttributesMetadataColumn = true
		}
	}
	return hasAttributesValuesColumn && hasAttributesMetadataColumn
}

func removePrecision(str string) string {
	if lastIndex := strings.LastIndex(str, "("); lastIndex != -1 {
		return str[:lastIndex]
	} else {
		return str
	}
}

func (td *tableDiscovery) readTables(database string) (map[string]map[string]columnMetadata, error) {
	logger.Debug().Msgf("describing tables: %s", database)

	if td.dbConnPool == nil {
		return map[string]map[string]columnMetadata{}, fmt.Errorf("database connection pool is nil, cannot describe tables")
	}

	rows, err := td.dbConnPool.Query(context.Background(), "SELECT table, name, type, comment FROM system.columns WHERE database = ?", database)

	if err != nil {
		err = end_user_errors.GuessClickhouseErrorType(err).InternalDetails("reading list of columns from system.columns")
		return map[string]map[string]columnMetadata{}, err
	}
	defer rows.Close()

	columnsPerTable := make(map[string]map[string]columnMetadata)
	for rows.Next() {
		var table, colName, colType, comment string
		if err := rows.Scan(&table, &colName, &colType, &comment); err != nil {
			return map[string]map[string]columnMetadata{}, err
		}
		if _, ok := columnsPerTable[table]; !ok {
			columnsPerTable[table] = make(map[string]columnMetadata)
		}
		columnsPerTable[table][colName] = columnMetadata{colType: colType, comment: comment}
	}

	if err := rows.Err(); err != nil {
		return map[string]map[string]columnMetadata{}, err
	}

	return columnsPerTable, nil
}

func (td *tableDiscovery) tableTimestampField(database, table string, dbKind DbKind) (primaryKey string) {
	switch dbKind {
	case Hydrolix:
		return td.getTimestampFieldForHydrolix(database, table)
	case ClickHouse:
		return td.getTimestampFieldForClickHouse(database, table)
	}
	return
}

func (td *tableDiscovery) getTimestampFieldForHydrolix(database, table string) (timestampField string) {
	// In Hydrolix, there's always only one column in a table set as a primary timestamp
	// Ref: https://docs.hydrolix.io/docs/transforms-and-write-schema#primary-timestamp
	err := td.dbConnPool.QueryRow(context.Background(), "SELECT primary_key FROM system.tables WHERE database = ? and table = ?", database, table).Scan(&timestampField)
	if err != nil {
		logger.Debug().Msgf("failed fetching primary key for table %s: %v", table, err)
	}
	return timestampField
}

func (td *tableDiscovery) getTimestampFieldForClickHouse(database, table string) (timestampField string) {
	// In ClickHouse, there's no concept of a primary timestamp field, primary keys are often composite,
	// hence we have to use following heuristic to determine the timestamp field (also just picking the first column if there are multiple)
	err := td.dbConnPool.QueryRow(context.Background(), "SELECT name FROM system.columns WHERE database = ? AND table = ? AND is_in_primary_key = 1 AND type iLIKE 'DateTime%'", database, table).Scan(&timestampField)
	if err != nil {
		logger.Debug().Msgf("failed fetching primary key for table %s: %v", table, err)
		return
	}
	return timestampField
}

func (td *tableDiscovery) tableComment(database, table string) (comment string) {
	err := td.dbConnPool.QueryRow(context.Background(), "SELECT comment FROM system.tables WHERE database = ? and table = ?", database, table).Scan(&comment)
	if err != nil {
		logger.Error().Msgf("could not get table comment: %v", err)
	}
	return comment
}

func (td *tableDiscovery) createTableQuery(database, table string) (ddl string) {
	rows, err := td.dbConnPool.Query(context.Background(), "SELECT create_table_query FROM system.tables WHERE database = ? and table = ? ", database, table)
	if err != nil {
		logger.Error().Msgf("could not get table comment: %v", err)
	}

	defer rows.Close()
	if scanErr := rows.Scan(&ddl); scanErr != nil {
		logger.Debug().Msgf("failed scanning primary key for table %s: %v", table, scanErr)
	}

	return ddl
}

type EmptyTableDiscovery struct {
	TableMap      *TableMap
	Err           error
	Autodiscovery bool
}

func NewEmptyTableDiscovery() *EmptyTableDiscovery {
	return &EmptyTableDiscovery{
		TableMap: NewTableMap(),
	}
}

func (td *EmptyTableDiscovery) ReloadTableDefinitions() {
}

func (td *EmptyTableDiscovery) TableDefinitions() *TableMap {
	return td.TableMap
}

func (td *EmptyTableDiscovery) TableDefinitionsFetchError() error {
	return td.Err
}

func (td *EmptyTableDiscovery) LastAccessTime() time.Time {
	return time.Now()
}

func (td *EmptyTableDiscovery) LastReloadTime() time.Time {
	return time.Now()
}

func (td *EmptyTableDiscovery) ForceReloadCh() <-chan chan<- struct{} {
	return make(chan chan<- struct{})
}

func (td *EmptyTableDiscovery) AutodiscoveryEnabled() bool {
	return td.Autodiscovery
}
