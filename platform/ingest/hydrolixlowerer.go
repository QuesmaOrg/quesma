// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/comment_metadata"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	chLib "github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/persistence"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/goccy/go-json"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type HydrolixLowerer struct {
	virtualTableStorage        persistence.JSONDatabase
	ingestCounter              atomic.Int64
	ingestFieldStatistics      IngestFieldStatistics
	ingestFieldStatisticsLock  sync.Mutex
	tableCreteStatementMapping map[*chLib.Table]CreateTableStatement // cache for table creation statements
	tableCreationLock          sync.Mutex
}

func NewHydrolixLowerer(virtualTableStorage persistence.JSONDatabase) *HydrolixLowerer {
	return &HydrolixLowerer{
		virtualTableStorage:        virtualTableStorage,
		tableCreteStatementMapping: make(map[*chLib.Table]CreateTableStatement),
	}
}
func (ip *HydrolixLowerer) shouldAlterColumns(table *chLib.Table, attrsMap map[string][]interface{}) (bool, []int) {
	attrKeys := getAttributesByArrayName(chLib.DeprecatedAttributesKeyColumn, attrsMap)
	alterColumnIndexes := make([]int, 0)

	// this is special case for common table storage
	// we do always add columns for common table storage
	if table.Name == common_table.TableName {
		if len(table.Cols) > alterColumnUpperLimit {
			logger.Warn().Msgf("Common table has more than %d columns (alwaysAddColumnLimit)", alterColumnUpperLimit)
		}
	}

	if len(table.Cols) < alwaysAddColumnLimit || table.Name == common_table.TableName {
		// We promote all non-schema fields to columns
		// therefore we need to add all attrKeys indexes to alterColumnIndexes
		for i := 0; i < len(attrKeys); i++ {
			alterColumnIndexes = append(alterColumnIndexes, i)
		}
		return true, alterColumnIndexes
	}

	if len(table.Cols) > alterColumnUpperLimit {
		return false, nil
	}
	ip.ingestFieldStatisticsLock.Lock()
	if ip.ingestFieldStatistics == nil {
		ip.ingestFieldStatistics = make(IngestFieldStatistics)
	}
	ip.ingestFieldStatisticsLock.Unlock()
	for i := 0; i < len(attrKeys); i++ {
		ip.ingestFieldStatisticsLock.Lock()
		ip.ingestFieldStatistics[IngestFieldBucketKey{indexName: table.Name, field: attrKeys[i]}]++
		counter := ip.ingestCounter.Add(1)
		fieldCounter := ip.ingestFieldStatistics[IngestFieldBucketKey{indexName: table.Name, field: attrKeys[i]}]
		// reset statistics every alwaysAddColumnLimit
		// for now alwaysAddColumnLimit is used in two contexts
		// for defining column limit and for resetting statistics
		if counter >= alwaysAddColumnLimit {
			ip.ingestCounter.Store(0)
			ip.ingestFieldStatistics = make(IngestFieldStatistics)
		}
		ip.ingestFieldStatisticsLock.Unlock()
		// if field is present more or equal fieldFrequency
		// during each alwaysAddColumnLimit iteration
		// promote it to column
		if fieldCounter >= fieldFrequency {
			alterColumnIndexes = append(alterColumnIndexes, i)
		}
	}
	if len(alterColumnIndexes) > 0 {
		return true, alterColumnIndexes
	}
	return false, nil
}

// This function generates ALTER TABLE commands for adding new columns
// to the table based on the attributesMap and the table name
// AttributesMap contains the attributes that are not part of the schema
// Function has side effects, it modifies the table.Cols map
// and removes the attributes that were promoted to columns
func (ip *HydrolixLowerer) generateNewColumns(
	attrsMap map[string][]interface{},
	table *chLib.Table,
	alteredAttributesIndexes []int,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName) []AlterStatement {
	var alterStatements []AlterStatement
	attrKeys := getAttributesByArrayName(chLib.DeprecatedAttributesKeyColumn, attrsMap)
	attrTypes := getAttributesByArrayName(chLib.DeprecatedAttributesValueType, attrsMap)
	var deleteIndexes []int

	reverseMap := reverseFieldEncoding(encodings, table.Name)

	// HACK Alert:
	// We must avoid altering the table.Cols map and reading at the same time.
	// This should be protected by a lock or a copy of the table should be used.
	//
	newColumns := make(map[string]*chLib.Column)
	for k, v := range table.Cols {
		newColumns[k] = v
	}

	for i := range alteredAttributesIndexes {

		columnType := ""
		modifiers := ""

		if attrTypes[i] == chLib.UndefinedType {
			continue
		}

		// Array and Map are not Nullable
		if strings.Contains(attrTypes[i], "Array") || strings.Contains(attrTypes[i], "Map") {
			columnType = attrTypes[i]
		} else {
			modifiers = "Nullable"
			columnType = fmt.Sprintf("Nullable(%s)", attrTypes[i])
		}

		propertyName := attrKeys[i]
		field, ok := reverseMap[schema.EncodedFieldName(attrKeys[i])]
		if ok {
			propertyName = field.FieldName
		}

		metadata := comment_metadata.NewCommentMetadata()
		metadata.Values[comment_metadata.ElasticFieldName] = propertyName
		comment := metadata.Marshall()

		alterColumn := AlterStatement{
			Type:       AddColumn,
			TableName:  table.Name,
			OnCluster:  table.ClusterName,
			ColumnName: attrKeys[i],
			ColumnType: columnType,
		}
		newColumns[attrKeys[i]] = &chLib.Column{Name: attrKeys[i], Type: chLib.NewBaseType(attrTypes[i]), Modifiers: modifiers, Comment: comment}
		alterStatements = append(alterStatements, alterColumn)

		alterColumnComment := AlterStatement{
			Type:       CommentColumn,
			TableName:  table.Name,
			OnCluster:  table.ClusterName,
			ColumnName: attrKeys[i],
			Comment:    comment,
		}
		alterStatements = append(alterStatements, alterColumnComment)

		deleteIndexes = append(deleteIndexes, i)
	}

	table.Cols = newColumns

	if table.VirtualTable {
		err := storeVirtualTable(table, ip.virtualTableStorage)
		if err != nil {
			logger.Error().Msgf("error storing virtual table: %v", err)
		}
	}

	for i := len(deleteIndexes) - 1; i >= 0; i-- {
		attrsMap[chLib.DeprecatedAttributesKeyColumn] = append(attrsMap[chLib.DeprecatedAttributesKeyColumn][:deleteIndexes[i]], attrsMap[chLib.DeprecatedAttributesKeyColumn][deleteIndexes[i]+1:]...)
		attrsMap[chLib.DeprecatedAttributesValueType] = append(attrsMap[chLib.DeprecatedAttributesValueType][:deleteIndexes[i]], attrsMap[chLib.DeprecatedAttributesValueType][deleteIndexes[i]+1:]...)
		attrsMap[chLib.DeprecatedAttributesValueColumn] = append(attrsMap[chLib.DeprecatedAttributesValueColumn][:deleteIndexes[i]], attrsMap[chLib.DeprecatedAttributesValueColumn][deleteIndexes[i]+1:]...)
	}
	return alterStatements
}

func (ip *HydrolixLowerer) GenerateIngestContent(table *chLib.Table,
	data types.JSON,
	inValidJson types.JSON,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName) ([]AlterStatement, types.JSON, []NonSchemaField, error) {

	if len(table.Config.Attributes) == 0 {
		return nil, data, nil, nil
	}

	mDiff := DifferenceMap(data, table) // TODO change to DifferenceMap(m, t)

	if len(mDiff) == 0 && len(inValidJson) == 0 { // no need to modify, just insert 'js'
		return nil, data, nil, nil
	}

	// check attributes precondition
	if len(table.Config.Attributes) <= 0 {
		return nil, nil, nil, fmt.Errorf("no attributes config, but received non-schema fields: %s", mDiff)
	}
	attrsMap, _ := BuildAttrsMap(mDiff, table.Config)

	// generateNewColumns is called on original attributes map
	// before adding invalid fields to it
	// otherwise it would contain invalid fields e.g. with wrong types
	// we only want to add fields that are not part of the schema e.g we don't
	// have columns for them
	var alterStatements []AlterStatement
	ip.ingestCounter.Add(1)
	if ok, alteredAttributesIndexes := ip.shouldAlterColumns(table, attrsMap); ok {
		alterStatements = ip.generateNewColumns(attrsMap, table, alteredAttributesIndexes, encodings)
	}
	// If there are some invalid fields, we need to add them to the attributes map
	// to not lose them and be able to store them later by
	// generating correct update query
	// addInvalidJsonFieldsToAttributes returns a new map with invalid fields added
	// this map is then used to generate non-schema fields string
	attrsMapWithInvalidFields := addInvalidJsonFieldsToAttributes(attrsMap, inValidJson)
	nonSchemaFields, err := generateNonSchemaFields(attrsMapWithInvalidFields)

	if err != nil {
		return nil, nil, nil, err
	}

	onlySchemaFields := RemoveNonSchemaFields(data, table)

	return alterStatements, onlySchemaFields, nonSchemaFields, nil
}

type TypeId int

const (
	PrimitiveType TypeId = iota
	ArrayType
	MapType
)

type TypeElement struct {
	Name       string
	IsNullable bool
}

type TypeInfo struct {
	TypeId     TypeId
	Elements   []TypeElement
	IsNullable bool
}

func GetTypeInfo(typeName string) TypeInfo {
	columnType := strings.TrimSpace(typeName)
	info := TypeInfo{}

	// Check for Nullable wrapper
	if strings.HasPrefix(columnType, "Nullable(") {
		info.IsNullable = true
		columnType = unwrapNullable(columnType)
	}

	// Parse Array or Map
	switch {
	case strings.HasPrefix(columnType, "Array("):
		info.TypeId = ArrayType
		inner := unwrapGeneric(columnType)
		info.Elements = []TypeElement{{Name: normalizeType(inner)}}

	case strings.HasPrefix(columnType, "Map("):
		info.TypeId = MapType
		inner := unwrapGeneric(columnType)
		parts := splitCommaArgs(inner)
		if len(parts) == 2 {
			info.Elements = []TypeElement{
				{Name: normalizeType(parts[0])},
				{Name: normalizeType(parts[1])},
			}
		}

	default:
		info.TypeId = PrimitiveType
		info.Elements = []TypeElement{{Name: normalizeType(columnType)}}
	}

	return info
}

// Unwraps e.g. Array(Float64) → Float64
func unwrapGeneric(s string) string {
	start := strings.Index(s, "(")
	end := strings.LastIndex(s, ")")
	if start >= 0 && end > start {
		return strings.TrimSpace(s[start+1 : end])
	}
	return s
}

// Splits arguments like Map(String, Int64)
func splitCommaArgs(s string) []string {
	var args []string
	var current strings.Builder
	var depth int
	for _, r := range s {
		switch r {
		case '(':
			depth++
			current.WriteRune(r)
		case ')':
			depth--
			current.WriteRune(r)
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(current.String()))
				current.Reset()
			} else {
				current.WriteRune(r)
			}
		default:
			current.WriteRune(r)
		}
	}
	if trimmed := strings.TrimSpace(current.String()); trimmed != "" {
		args = append(args, trimmed)
	}
	return args
}

// Normalize ClickHouse-like types
func normalizeType(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	switch {
	case strings.Contains(t, "float64"):
		return "double"
	case strings.Contains(t, "datetime"):
		return "datetime"
	}
	return t
}

// Removes Nullable(...) and returns the inner string
func unwrapNullable(s string) string {
	if strings.HasPrefix(s, "Nullable(") && strings.HasSuffix(s, ")") {
		return strings.TrimSpace(s[9 : len(s)-1])
	}
	return s
}

func defaultForType(t string) interface{} {
	switch t {
	case "string":
		return ""
	case "int64":
		return int64(123)
	case "uint64":
		return uint64(123)
	case "uint32":
		return uint32(123)
	case "double", "float64":
		return "1.23"
	case "datetime":
		return "2020-02-26 16:01:27 PST"
	case "bool":
		return true
	default:
		return nil
	}
}

func parseFlexibleTime(input string) (time.Time, error) {
	// First try RFC3339 (with timezone)
	t, err := time.Parse(time.RFC3339, input)
	if err == nil {
		return t, nil
	}

	// Fallback: try without timezone and assume UTC
	layout := "2006-01-02T15:04:05"
	return time.ParseInLocation(layout, input, time.UTC)
}

func CastToType(value any, typeName string) (any, error) {
	switch typeName {
	case "string":
		if v, ok := value.(string); ok {
			return v, nil
		}
		return fmt.Sprintf("%v", value), nil

	case "int":
		if v, ok := value.(int); ok {
			return v, nil
		}
		switch v := value.(type) {
		case float64:
			return int(v), nil
		case string:
			return strconv.Atoi(v)
		}

	case "float64", "double":
		if v, ok := value.(float64); ok {
			return v, nil
		}
		switch v := value.(type) {
		case int:
			return float64(v), nil
		case string:
			return strconv.ParseFloat(v, 64)
		}

	case "bool":
		if v, ok := value.(bool); ok {
			return v, nil
		}
		switch v := value.(type) {
		case string:
			return strconv.ParseBool(v)
		}
	case "int64":
		if v, ok := value.(int64); ok {
			return v, nil
		}
		switch v := value.(type) {
		case float64:
			return int64(v), nil
		case string:
			return strconv.Atoi(v)
		}
	case "datetime":
		if v, ok := value.(string); ok {

			parsedTime, err := parseFlexibleTime(v)
			if err != nil {
				fmt.Println("Error parsing time:", err)
				return nil, err
			}
			return parsedTime.Format("2006-01-02 15:04:05 MST"), nil

		}
	default:
		return nil, fmt.Errorf("unsupported target type: %s", typeName)
	}

	return nil, fmt.Errorf("cannot convert %T to %s", value, typeName)
}

func (l *HydrolixLowerer) LowerToDDL(
	validatedJsons []types.JSON,
	table *chLib.Table,
	invalidJsons []types.JSON,
	encodings map[schema.FieldEncodingKey]schema.EncodedFieldName,
	createTableCmd CreateTableStatement,
) ([]string, error) {

	l.tableCreationLock.Lock()
	if _, exists := l.tableCreteStatementMapping[table]; !exists {
		l.tableCreteStatementMapping[table] = createTableCmd
	} else {
		createTableCmd = l.tableCreteStatementMapping[table]
	}
	l.tableCreationLock.Unlock()

	// --- Create Table Section ---
	createTable := map[string]interface{}{
		"name": table.Name,
		"settings": map[string]interface{}{
			"merge": map[string]interface{}{
				"enabled": true,
			},
		},
	}

	// --- Output Columns Slice ---
	outputColumns := make([]interface{}, 0)

	for _, col := range createTableCmd.Columns {
		typeInfo := GetTypeInfo(col.ColumnType)

		// Build base datatype map
		datatype := map[string]interface{}{
			"type": typeInfo.Elements[0].Name, // For primitive, or outer type for array/map
		}

		// Nullable handling
		if typeInfo.IsNullable {
			datatype["denullify"] = false
		}

		// Primary timestamp column
		if col.ColumnName == "@timestamp" {
			datatype["primary"] = true
		}

		// Add format for datetime
		if datatype["type"] == "datetime" {
			datatype["format"] = "2006-01-02 15:04:05 MST"
		}

		// Handle array elements
		if typeInfo.TypeId == ArrayType && len(typeInfo.Elements) > 0 {
			datatype["type"] = "array"
			elementType := normalizeType(typeInfo.Elements[0].Name)
			element := map[string]interface{}{
				"type": elementType,
				"index_options": map[string]interface{}{
					"fulltext": false,
				},
			}

			if elementType == "datetime" {
				element["format"] = "2006-01-02 15:04:05 MST"
			}

			datatype["elements"] = []interface{}{element}
		}

		// Handle map elements
		if typeInfo.TypeId == MapType && len(typeInfo.Elements) == 2 {
			datatype["type"] = "map"
			keyType := normalizeType(typeInfo.Elements[0].Name)
			valueType := normalizeType(typeInfo.Elements[1].Name)

			element1 := map[string]interface{}{
				"type": keyType,
				"index_options": map[string]interface{}{
					"fulltext": false,
				},
			}

			if keyType == "datetime" {
				element1["format"] = "2006-01-02 15:04:05 MST"
			}
			element2 := map[string]interface{}{
				"type": valueType,
				"index_options": map[string]interface{}{
					"fulltext": false,
				},
			}

			if valueType == "datetime" {
				element2["format"] = "2006-01-02 15:04:05 MST"
			}
			datatype["elements"] = []interface{}{element1, element2}
		}

		// Final column map
		columnMap := map[string]interface{}{
			"name":     col.ColumnName,
			"datatype": datatype,
		}

		outputColumns = append(outputColumns, columnMap)
	}

	// --- Transform Section ---
	transform := map[string]interface{}{
		"name": "transform1",
		"type": "json",
		"settings": map[string]interface{}{
			"format_details": map[string]interface{}{
				"flattening": map[string]interface{}{
					"active": false,
				},
			},
			"output_columns": outputColumns,
		},
	}

	// --- Ingest Section ---
	ingestSlice := make([]map[string]interface{}, 0)

	for i, preprocessedJson := range validatedJsons {
		_, onlySchemaFields, nonSchemaFields, err := l.GenerateIngestContent(table, preprocessedJson,
			invalidJsons[i], encodings)
		if err != nil {
			return nil, fmt.Errorf("error BuildInsertJson, tablename: '%s' : %v", table.Name, err)
		}
		events := convertNonSchemaFieldsToMap(nonSchemaFields)

		for k, v := range onlySchemaFields {
			events[k] = v
		}
		ingest := map[string]interface{}{}

		for _, col := range createTableCmd.Columns {
			colName := col.ColumnName

			typeInfo := GetTypeInfo(col.ColumnType)

			var value interface{}

			switch typeInfo.TypeId {
			case PrimitiveType:
				if _, exists := events[colName]; !exists {
					value = defaultForType(typeInfo.Elements[0].Name)
				} else {
					val, _ := CastToType(events[colName], typeInfo.Elements[0].Name)
					value = val

				}

			case ArrayType:
				elemType := typeInfo.Elements[0].Name
				value = []any{}
				if events[colName] != nil {
					for _, elem := range events[colName].([]any) {
						castedElem, err := CastToType(elem, elemType)
						if err != nil {
							logger.ErrorWithCtx(context.Background()).Msgf("Error casting element %v to type %s: %v", elem, elemType, err)
							continue
						}
						value = append(value.([]interface{}), castedElem)
					}
				}
			case MapType:
				if events[colName] != nil {
					rawMap, ok := events[colName].(map[string]any)
					if ok {
						valType := typeInfo.Elements[1].Name
						typedMap := make(map[string]any)
						for rawKey, rawVal := range rawMap {
							castedVal, err := CastToType(rawVal, valType)
							if err != nil {
								logger.ErrorWithCtx(context.Background()).
									Msgf("Error casting map value %v to type %s: %v", rawVal, valType, err)
								continue
							}
							typedMap[rawKey] = castedVal
						}
						value = typedMap
					}
				}
			}

			ingest[colName] = value
		}
		if len(ingest) > 0 {
			ingestSlice = append(ingestSlice, ingest)
		}
	}
	// --- Final Payload ---
	// There is implicit interface here between lowerer and backend connector
	// so we need to generate payload that is compatible with backend connector
	// backend connector expects a specific structure
	payload := map[string]interface{}{
		"create_table": createTable,
		"transform":    transform,
		"ingest":       ingestSlice,
	}
	logger.InfoWithCtx(context.Background()).Msgf("Ingesting %d %d %d events into table %s", len(validatedJsons), len(createTableCmd.Columns), len(ingestSlice), table.Name)
	marshaledPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("error marshalling payload: %v", err)
	}
	return []string{string(marshaledPayload)}, nil

}
