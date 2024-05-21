package clickhouse

import (
	"mitmproxy/quesma/logger"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/util"
	"strings"
	"sync/atomic"
)

type schemaLoader struct {
	cfg              config.QuesmaConfiguration
	SchemaManagement *SchemaManagement
	tableDefinitions *atomic.Pointer[TableMap]
}

func (sl *schemaLoader) ReloadTables() {
	logger.Debug().Msg("reloading tables definitions")
	configuredTables := make(map[string]discoveredTable)
	var explicitlyDisabledTables, notConfiguredTables []string
	databaseName := "default"
	if sl.cfg.ClickHouse.Database != "" {
		databaseName = sl.cfg.ClickHouse.Database
	}
	if tables, err := sl.SchemaManagement.readTables(databaseName); err != nil {
		logger.Error().Msgf("could not describe tables: %v", err)
		return
	} else {
		for table, columns := range tables {
			if indexConfig, found := sl.cfg.IndexConfig[table]; found {
				if indexConfig.Enabled {
					// alias collision, likely will go away
					for colName := range columns {
						if field, exists := indexConfig.Fields[colName]; exists && field.Type != nil && *field.Type == "alias" {
							logger.Error().Msgf("column [%s] clashes with an existing alias, table [%s]", colName, table)
						}
					}
					comment := sl.SchemaManagement.tableComment(databaseName, table)
					createTableQuery := sl.SchemaManagement.createTableQuery(databaseName, table)
					configuredTables[table] = discoveredTable{columns, indexConfig, comment, createTableQuery}
				} else {
					explicitlyDisabledTables = append(explicitlyDisabledTables, table)
				}
			} else {
				notConfiguredTables = append(notConfiguredTables, table)
			}
		}
	}
	logger.Info().Msgf(
		"Table discovery results: configured=[%s], found but not configured=[%s], explicitly disabled=[%s]",
		strings.Join(util.MapKeys(configuredTables), ","),
		strings.Join(notConfiguredTables, ","),
		strings.Join(explicitlyDisabledTables, ","),
	)

	sl.populateTableDefinitions(configuredTables, databaseName, sl.cfg)
}

func (sl *schemaLoader) populateTableDefinitions(configuredTables map[string]discoveredTable, databaseName string, cfg config.QuesmaConfiguration) {
	tableMap := NewTableMap()
	for tableName, resTable := range configuredTables {
		var columnsMap = make(map[string]*Column)
		partiallyResolved := false
		for col, colType := range resTable.columnTypes {

			if field, isIgnored := resTable.config.Fields[col]; isIgnored && field.Type != nil && *field.Type == "alias" {
				logger.Debug().Msgf("table %s, column %s is ignored", tableName, col)
				continue
			}
			if col != AttributesKeyColumn && col != AttributesValueColumn {
				column := resolveColumn(col, colType)
				if column != nil {
					columnsMap[col] = column
				} else {
					logger.Debug().Msgf("column %s, %s not resolved", col, colType)
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

func (sl *schemaLoader) TableDefinitions() *TableMap {
	return sl.tableDefinitions.Load()
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
