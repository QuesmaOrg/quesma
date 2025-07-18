// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package database_common

import (
	"context"
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/end_user_errors"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/persistence"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/QuesmaOrg/quesma/platform/util"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/goccy/go-json"
	"regexp"
	"strings"
	"sync"
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
	AddTable(tableName string, table *Table)
	TableDefinitionsFetchError() error

	LastAccessTime() time.Time
	LastReloadTime() time.Time
	ForceReloadCh() <-chan chan<- struct{}
	AutodiscoveryEnabled() bool

	RegisterTablesReloadListener(ch chan<- types.ReloadMessage)
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

	reloadObserversMutex sync.Mutex
	reloadObservers      []chan<- types.ReloadMessage
}

type columnMetadata struct {
	colType string
	// currently column contains original field value
	// we use it as persistent storage and load it
	// in the case when we don't control ingest
	comment string
	origin  schema.FieldSource // TODO this field is just added to have way to forward information to the schema registry and should be considered as a technical debt
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

func (t TableDiscoveryTableProviderAdapter) RegisterTablesReloadListener(ch chan<- types.ReloadMessage) {
	t.TableDiscovery.RegisterTablesReloadListener(ch)
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
				Origin:  column.Origin,
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

func (td *tableDiscovery) AddTable(tableName string, table *Table) {
	td.tableDefinitions.Load().Store(tableName, table)
	td.notifyObservers()
}

func (td *tableDiscovery) RegisterTablesReloadListener(ch chan<- types.ReloadMessage) {
	td.reloadObserversMutex.Lock()
	defer td.reloadObserversMutex.Unlock()
	td.reloadObservers = append(td.reloadObservers, ch)
}

func (td *tableDiscovery) notifyObservers() {

	td.reloadObserversMutex.Lock()
	defer td.reloadObserversMutex.Unlock()

	msg := types.ReloadMessage{Timestamp: time.Now()}
	for _, observer := range td.reloadObservers {
		logger.Info().Msgf("Sending message to observer %v", observer)
		go func() {
			observer <- msg
		}()
	}
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
		if td.cfg.MapFieldsDiscoveringEnabled {
			tables = td.enrichTableWithMapFields(tables)
		}
		if td.AutodiscoveryEnabled() {
			configuredTables = td.autoConfigureTables(tables, databaseName)
		} else {
			configuredTables = td.configureTables(tables, databaseName)
		}
	}
	var tablePresence map[string][]TablePresence
	if td.cfg.ClusterName != "" {
		var err error
		tablePresence, err = td.getTablePresenceAcrossClusters(databaseName)
		if err != nil {
			logger.Warn().Msgf("could not get table presence across clusters: %v", err)
		}
		logger.Info().Msgf("Table presence across clusters: %v", tablePresence)
		configuredTables = td.populateClusterNodes(configuredTables, databaseName, tablePresence)
	}

	configuredTables = td.readVirtualTables(configuredTables)

	td.ReloadTablesError = nil
	td.populateTableDefinitions(configuredTables, databaseName, td.cfg)

	td.notifyObservers()
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
	overrideToOriginal := make(map[string]string)

	// populate map of override to original table names
	// this will be further used to take specific index config
	// from the original table
	for indexName, indexConfig := range td.cfg.IndexConfig {
		if len(indexConfig.Override) > 0 {
			overrideToOriginal[indexConfig.Override] = indexName
		}
	}

	for table, columns := range tables {

		// single logs table is our internal table, user shouldn't configure it at all
		// and we should always include it in the list of tables managed by Quesma
		isCommonTable := table == common_table.TableName
		override := false
		if _, found := overrideToOriginal[table]; found {
			override = true
		}
		if indexConfig, found := td.cfg.IndexConfig[table]; found || isCommonTable || override {

			if isCommonTable {
				indexConfig = config.IndexConfiguration{}
			}
			// if table is overridden, we take the index config from the original index
			if override {
				indexConfig = td.cfg.IndexConfig[overrideToOriginal[table]]
			}
			if !isCommonTable && !indexConfig.IsClickhouseQueryEnabled() && !indexConfig.IsClickhouseIngestEnabled() {
				explicitlyDisabledTables = append(explicitlyDisabledTables, table)
			} else {
				comment := td.tableComment(databaseName, table)
				createTableQuery := td.createTableQuery(databaseName, table)
				// we assume here that @timestamp field is always present in the table, or it's explicitly configured
				configuredTables[table] = discoveredTable{table, databaseName, columns, indexConfig, comment, createTableQuery, "", false, false}
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
		configuredTables[table] = discoveredTable{table, databaseName, columns, config.IndexConfiguration{}, comment, createTableQuery, maybeTimestampField, isVirtualTable, false}

	}
	for tableName, table := range configuredTables {
		autoDiscoResults.WriteString(fmt.Sprintf("{table: %s, timestampField: %s}, ", tableName, table.timestampFieldName))
	}
	logger.Info().Msgf("Table auto-discovery results -> %d tables found: [%s]", len(configuredTables), strings.TrimSuffix(autoDiscoResults.String(), ", "))
	return
}

func (td *tableDiscovery) populateTableDefinitions(configuredTables map[string]discoveredTable, databaseName string, cfg *config.QuesmaConfiguration) {
	instanceType := GetInstanceType(td.dbConnPool.InstanceName())

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

			column := ResolveColumn(col, columnMeta.colType, instanceType)
			if column != nil {
				column.Comment = columnMeta.comment
				column.Origin = columnMeta.origin
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
				Name:         tableName,
				Comment:      resTable.comment,
				DatabaseName: databaseName,
				ClusterName:  cfg.ClusterName, // FIXME: is this really necessary? The cluster name is only used when creating table, but this is an already created table - so is this information not needed?
				Cols:         columnsMap,
				Config: &ChTableConfig{
					Attributes:                            []Attribute{},
					CastUnsupportedAttrValueTypesToString: true,
					PreferCastingToOthers:                 true,
				},
				CreateTableQuery:             resTable.createTableQuery,
				DiscoveredTimestampFieldName: timestampFieldName,
				VirtualTable:                 resTable.virtualTable,
				ExistsOnAllNodes:             resTable.existsOnAllNodes,
			}

			// We're adding default attributes to the virtual tables. We store virtual tables in the elastic as a list of essential column names.
			// Quesma heavily relies on the attributes when it alters schema on ingest (see processor.go)
			// If we don't add attributes to the virtual tables, virtual tables will be not altered on ingest.
			if containsAttributes(resTable.columnTypes) || resTable.virtualTable {
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

func ResolveColumn(colName, colType string, instanceType InstanceType) *Column {
	isNullable := false
	if isNullableType(colType) {
		isNullable = true
		colType = strings.TrimSuffix(strings.TrimPrefix(colType, "Nullable("), ")")
	}
	r := GetTypeResolver(instanceType)

	if isArrayType(colType) {
		arrayType := strings.TrimSuffix(strings.TrimPrefix(colType, "Array("), ")")
		if isNullableType(arrayType) {
			isNullable = true
			arrayType = strings.TrimSuffix(strings.TrimPrefix(arrayType, "Nullable("), ")")
		}
		if isArrayType(arrayType) {
			innerColumn := ResolveColumn("inner", arrayType, instanceType)
			if innerColumn == nil {
				logger.Warn().Msgf("invalid inner array type for column %s, %s", colName, colType)
				return nil
			}
			return &Column{
				Name: colName,
				Type: CompoundType{
					Name:     "Array",
					BaseType: innerColumn.Type,
				},
			}
		}
		GoType := r.ResolveType(arrayType)
		if GoType != nil {
			return &Column{
				Name: colName,
				Type: CompoundType{
					Name:     "Array",
					BaseType: BaseType{Name: arrayType, GoType: GoType, Nullable: isNullable},
				},
			}
		} else if isTupleType(arrayType) {
			tupleColumn := ResolveColumn("Tuple", arrayType, instanceType)
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
				GoType: NewBaseTypeWithInstanceName("Int32", instanceType).GoType,
			},
		}
	}

	// It's not array or tuple -> it's base type
	if strings.HasPrefix(colType, "DateTime") {
		colType = removePrecision(colType)
	}
	if GoType := r.ResolveType(colType); GoType != nil {
		return &Column{
			Name: colName,
			Type: BaseType{
				Name:     colType,
				GoType:   NewBaseTypeWithInstanceName(colType, instanceType).GoType,
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
				GoType:   NewBaseTypeWithInstanceName("Unknown", instanceType).GoType,
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

// extractMapValueType extracts the value type from a ClickHouse Map definition
// extractMapValueType extracts the value type from a ClickHouse Map definition.
func extractMapValueType(mapType string) (string, error) {
	// Regular expression to match "Map(String, <valueType>)"
	re := regexp.MustCompile(`Map\((?:LowCardinality\()?String\)?,\s*(.+)\)$`)

	matches := re.FindStringSubmatch(mapType)
	if len(matches) < 2 {
		return "", errors.New("invalid map type format: " + mapType)
	}

	// Trim spaces and return the full value type
	return strings.TrimSpace(matches[1]), nil
}

func (td *tableDiscovery) enrichTableWithMapFields(inputTable map[string]map[string]columnMetadata) map[string]map[string]columnMetadata {
	outputTable := make(map[string]map[string]columnMetadata)
	for table, columns := range inputTable {
		for colName, columnMeta := range columns {
			columnType := strings.TrimSpace(columnMeta.colType)
			if strings.HasPrefix(columnType, "Map(String") ||
				strings.HasPrefix(columnType, "Map(LowCardinality(String") {
				logger.Debug().Msgf("Discovered map column: %s.%s", table, colName)
				// Ensure the table exists in outputTable
				if _, ok := outputTable[table]; !ok {
					outputTable[table] = make(map[string]columnMetadata)
				}
				if _, ok := outputTable[table][colName]; !ok {
					// Update origin for incoming map column
					columnMeta.origin = schema.FieldSourceIngest
					outputTable[table][colName] = columnMeta
					logger.Debug().Msgf("Added column: %s.%s", table, colName)
				}

				// Query ClickHouse for map keys in the given column
				rows, err := td.dbConnPool.Query(context.Background(), fmt.Sprintf("SELECT DISTINCT arrayJoin(mapKeys(%s)) FROM %s", colName, table))
				if err != nil {
					logger.Error().Msgf("Error querying map keys for table, column: %s, %s, %v", table, colName, err)
					continue
				}
				foundKeys := false
				for rows.Next() {
					foundKeys = true
					var key string
					if err := rows.Scan(&key); err != nil {
						logger.Error().Msgf("Error scanning key for table, column: %s, %s, %v", table, colName, err)
						continue
					}
					// Add virtual column for each key in the map
					// with origin set to mapping
					mapKeyCol := colName + "." + key
					var valueType string
					valueType, err = extractMapValueType(columnType)
					if err != nil {
						logger.Error().Msgf("Error extracting value type for table, column: %s, %s, %v", table, colName, err)
						continue
					} else {
						outputTable[table][mapKeyCol] = columnMetadata{
							colType: valueType,
							origin:  schema.FieldSourceMapping,
						}
						logger.Debug().Msgf("Added map key column: %s.%s", table, mapKeyCol)
					}
				}
				if !foundKeys {
					logger.Debug().Msgf("No map keys found for table, column: %s, %s", table, colName)
				}
				if err := rows.Err(); err != nil {
					logger.Error().Msgf("Error iterating map keys for %s.%s: %v", table, colName, err)
				}
				err = rows.Close() // Close after processing
				if err != nil {
					logger.Error().Msgf("Error closing rows for table, column: %s, %s, %v", table, colName, err)
				}
			} else {
				// Copy other columns as-is
				if _, ok := outputTable[table]; !ok {
					outputTable[table] = make(map[string]columnMetadata)
				}
				outputTable[table][colName] = columnMeta
			}
		}
	}
	return outputTable
}

type TablePresence struct {
	Database         string
	Table            string
	FoundNodes       int
	TotalNodes       int
	ExistsOnAllNodes bool
}

func (td *tableDiscovery) getTablePresenceAcrossClusters(database string) (map[string][]TablePresence, error) {
	// Step 1: Get all cluster names
	clusterQuery := `SELECT DISTINCT cluster FROM system.clusters ORDER BY cluster`
	rows, err := td.dbConnPool.Query(context.Background(), clusterQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query cluster list: %w", err)
	}
	defer rows.Close()

	var clusters []string
	for rows.Next() {
		var cluster string
		if err := rows.Scan(&cluster); err != nil {
			return nil, fmt.Errorf("failed to scan cluster name: %w", err)
		}
		clusters = append(clusters, cluster)
	}

	// Step 2: For each cluster, safely query for the given database
	presenceData := make(map[string][]TablePresence)

	for _, cluster := range clusters {
		query := `
            WITH (
                SELECT count(DISTINCT host_name)
                FROM system.clusters
                WHERE cluster = ?
            ) AS total_nodes

            SELECT
                database,
                name AS table_name,
                count(DISTINCT hostName()) AS found_nodes,
                total_nodes,
                count(DISTINCT hostName()) = total_nodes AS exists_on_all_nodes
            FROM cluster(?, system.tables)
            WHERE database = ?
            GROUP BY database, name, total_nodes
        `

		rows, err := td.dbConnPool.Query(context.Background(), query, cluster, cluster, database)
		if err != nil {
			return nil, fmt.Errorf("failed to query tables for cluster %s: %w", cluster, err)
		}
		defer rows.Close()

		var tables []TablePresence
		for rows.Next() {
			var tp TablePresence
			if err := rows.Scan(&tp.Database, &tp.Table, &tp.FoundNodes, &tp.TotalNodes, &tp.ExistsOnAllNodes); err != nil {
				return nil, fmt.Errorf("failed to scan table row: %w", err)
			}
			tables = append(tables, tp)
		}

		if len(tables) > 0 {
			presenceData[cluster] = tables
		}
	}

	return presenceData, nil
}

func (td *tableDiscovery) populateClusterNodes(configuredTables map[string]discoveredTable, databaseName string, tablePresence map[string][]TablePresence) map[string]discoveredTable {
	for _, tables := range tablePresence {
		for _, table := range tables {
			if table.Database == databaseName {
				if discoTable, ok := configuredTables[table.Table]; ok {
					discoTable.existsOnAllNodes = table.ExistsOnAllNodes
				}
			}
		}
	}
	return configuredTables
}

func (td *tableDiscovery) readTables(database string) (map[string]map[string]columnMetadata, error) {
	logger.Debug().Msgf("describing tables: %s", database)

	if td.dbConnPool == nil {
		return map[string]map[string]columnMetadata{}, fmt.Errorf("database connection pool is nil, cannot describe tables")
	}
	var querySql string
	if td.dbConnPool.InstanceName() == "doris" {
		querySql = fmt.Sprintf("SELECT table_name, column_name, data_type, column_comment FROM information_schema.columns WHERE table_schema = '%s'", database)
	} else {
		querySql = fmt.Sprintf("SELECT table, name, type, comment FROM system.columns WHERE database = '%s'", database)
	}
	rows, err := td.dbConnPool.Query(context.Background(), querySql)

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
	if td.dbConnPool.InstanceName() == "doris" {
		// todo add doris comment
		return comment
	}
	err := td.dbConnPool.QueryRow(context.Background(), "SELECT comment FROM system.tables WHERE database = ? and table = ?", database, table).Scan(&comment)
	if err != nil {
		logger.Error().Msgf("could not get table comment: %v", err)
	}
	return comment
}

func (td *tableDiscovery) createTableQuery(database, table string) (ddl string) {
	if td.dbConnPool.InstanceName() == "doris" {
		// todo add doris ddl
		return ddl
	}
	err := td.dbConnPool.QueryRow(context.Background(), "SELECT create_table_query FROM system.tables WHERE database = ? and table = ? ", database, table).Scan(&ddl)
	if err != nil {
		logger.Error().Msgf("could not get create table statement: %v", err)
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

func (td *EmptyTableDiscovery) RegisterTablesReloadListener(ch chan<- types.ReloadMessage) {
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

func (td *EmptyTableDiscovery) AddTable(tableName string, table *Table) {
	td.TableMap.Store(tableName, table)
}
