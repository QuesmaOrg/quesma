// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"context"
	"fmt"
	chLib "github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/persistence"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/types"
	"github.com/goccy/go-json"
	"strconv"
	"strings"
	"sync/atomic"
)

type HydrolixLowerer struct {
	virtualTableStorage        persistence.JSONDatabase
	ingestCounter              atomic.Int64
	tableCreteStatementMapping map[*chLib.Table]CreateTableStatement // cache for table creation statements
}

func NewHydrolixLowerer(virtualTableStorage persistence.JSONDatabase) *HydrolixLowerer {
	return &HydrolixLowerer{
		virtualTableStorage:        virtualTableStorage,
		tableCreteStatementMapping: make(map[*chLib.Table]CreateTableStatement),
	}
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
	//if ok, alteredAttributesIndexes := ip.shouldAlterColumns(table, attrsMap); ok {
	//	alterStatements = ip.generateNewColumns(attrsMap, table, alteredAttributesIndexes, encodings)
	//}
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

	if _, exists := l.tableCreteStatementMapping[table]; !exists {
		l.tableCreteStatementMapping[table] = createTableCmd
	} else {
		createTableCmd = l.tableCreteStatementMapping[table]
	}

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
	ingests := make([]map[string]interface{}, 0)

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
					if typeInfo.Elements[0].Name == "datetime" {
						value = defaultForType(typeInfo.Elements[0].Name)
					} else {
						val, _ := CastToType(events[colName], typeInfo.Elements[0].Name)
						value = val //defaultForType(typeInfo.Elements[0].Name)
					}
				}

			case ArrayType:
				elemType := typeInfo.Elements[0].Name
				value = []interface{}{defaultForType(elemType)} // array with one sample element

			case MapType:
				keyType := typeInfo.Elements[0].Name
				valType := typeInfo.Elements[1].Name
				value = map[string]interface{}{
					fmt.Sprintf("%v", defaultForType(keyType)): defaultForType(valType),
				}
			}

			ingest[colName] = value
		}
		if len(ingest) > 0 {
			ingests = append(ingests, ingest)
		}
	}
	// --- Final Payload ---
	payload := map[string]interface{}{
		"create_table": createTable,
		"transform":    transform,
		"ingest":       ingests,
	}
	logger.InfoWithCtx(context.Background()).Msgf("Ingesting %d %d %d events into table %s", len(validatedJsons), len(createTableCmd.Columns), len(ingests), table.Name)
	marshaledPayload, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("error marshalling payload: %v", err)
	}
	return []string{string(marshaledPayload)}, nil

}
