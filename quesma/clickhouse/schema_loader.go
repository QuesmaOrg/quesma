package clickhouse

import (
	"mitmproxy/quesma/logger"
	"strings"
)

func populateTableDefinitions(configuredTables map[string]map[string]string, databaseName string, lm *LogManager) {
	tableMap := withPredefinedTables()
	for tableName, columns := range configuredTables {
		var columnsMap = make(map[string]*Column)
		partiallyResolved := false
		for col, colType := range columns {
			if col != attributesKeyColumn && col != attributesValueColumn {
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
				DatabaseName: databaseName,
				Cols:         columnsMap,
				Config: &ChTableConfig{
					attributes:                            []Attribute{},
					castUnsupportedAttrValueTypesToString: true,
					preferCastingToOthers:                 true,
				},
			}
			if lm.containsAttributes(columns) {
				table.Config.attributes = []Attribute{NewDefaultStringAttribute()}
			}

			table.applyFullTextSearchConfig(lm.cfg)

			tableMap.Store(tableName, &table)

			logger.Info().Msgf("schema for table [%s] loaded", tableName)
		} else {
			logger.Warn().Msgf("table %s not fully resolved, skipping", tableName)
		}
	}

	lm.tableDefinitions.Store(&tableMap)
}

func resolveColumn(colName, colType string) *Column {
	isNullable := false
	if isNullableType(colType) {
		isNullable = true
		colType = strings.TrimSuffix(strings.TrimPrefix(colType, "Nullable("), ")")
	}

	if isArrayType(colType) {
		arrayType := strings.TrimSuffix(strings.TrimPrefix(colType, "Array("), ")")
		goType := ResolveType(arrayType)
		if goType != nil {
			return &Column{
				Name: colName,
				Type: CompoundType{
					Name:     "Array",
					BaseType: BaseType{Name: arrayType, goType: goType},
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
	}

	_ = isNullable

	// TODO nullable

	if strings.HasPrefix(colType, "DateTime") {
		colType = removePrecision(colType)
	}
	if goType := ResolveType(colType); goType != nil {
		return &Column{
			Name: colName,
			Type: BaseType{
				Name:   colType,
				goType: NewBaseType(colType).goType,
			},
		}
	} else {
		logger.Error().Msgf("unknown type: %s, resolving to nil", colType)
		return nil
	}
}

func isArrayType(colType string) bool {
	return strings.HasPrefix(colType, "Array(") && strings.HasSuffix(colType, ")")
}

func isTupleType(colType string) bool {
	return strings.HasPrefix(colType, "Tuple(") && strings.HasSuffix(colType, ")")
}

func isNullableType(colType string) bool {
	return strings.HasPrefix(colType, "Nullable(")
}

func (lm *LogManager) containsAttributes(cols map[string]string) bool {
	hasAttributesKey := false
	hasAttributesValues := false
	for col, colType := range cols {
		if col == attributesKeyColumn && colType == attributesColumnType {
			hasAttributesKey = true
		}
		if col == attributesValueColumn && colType == attributesColumnType {
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
