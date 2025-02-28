// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/QuesmaOrg/quesma/quesma/comment_metadata"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/schema"
	"github.com/QuesmaOrg/quesma/quesma/util"
	"slices"
	"strings"
)

const NestedSeparator = "::"

type CreateTableEntry struct {
	ClickHouseColumnName string
	ClickHouseType       string
}

func reverseFieldEncoding(fieldEncodings map[schema.FieldEncodingKey]schema.EncodedFieldName, tableName string) map[schema.EncodedFieldName]schema.FieldEncodingKey {
	res := make(map[schema.EncodedFieldName]schema.FieldEncodingKey)

	for k, v := range fieldEncodings {
		if k.TableName == tableName {
			res[v] = k
		}
	}
	return res
}

// Rendering columns to string
func columnsToString(columnsFromJson []CreateTableEntry,
	columnsFromSchema map[schema.FieldName]CreateTableEntry,
	fieldEncodings map[schema.FieldEncodingKey]schema.EncodedFieldName,
	tableName string,
) string {

	reverseMap := reverseFieldEncoding(fieldEncodings, tableName)

	var result strings.Builder
	first := true
	for _, columnFromJson := range columnsFromJson {
		if first {
			first = false
		} else {
			result.WriteString(",\n")
		}
		result.WriteString(util.Indent(1))

		propertyName := reverseMap[schema.EncodedFieldName(columnFromJson.ClickHouseColumnName)].FieldName

		columnMetadata := comment_metadata.NewCommentMetadata()
		columnMetadata.Values[comment_metadata.ElasticFieldName] = propertyName
		comment := columnMetadata.Marshall()

		if columnFromSchema, found := columnsFromSchema[schema.FieldName(columnFromJson.ClickHouseColumnName)]; found && !strings.Contains(columnFromJson.ClickHouseType, "Array") {
			// Schema takes precedence over JSON (except for Arrays which are not currently handled)
			result.WriteString(fmt.Sprintf("\"%s\" %s '%s'", columnFromSchema.ClickHouseColumnName, columnFromSchema.ClickHouseType+" COMMENT ", comment))
		} else {
			result.WriteString(fmt.Sprintf("\"%s\" %s '%s'", columnFromJson.ClickHouseColumnName, columnFromJson.ClickHouseType+" COMMENT ", comment))
		}

		delete(columnsFromSchema, schema.FieldName(columnFromJson.ClickHouseColumnName))
	}

	// There might be some columns from schema which were not present in the JSON
	for _, column := range columnsFromSchema {
		if first {
			first = false
		} else {
			result.WriteString(",\n")
		}

		propertyName := reverseMap[schema.EncodedFieldName(column.ClickHouseColumnName)].FieldName

		columnMetadata := comment_metadata.NewCommentMetadata()
		columnMetadata.Values[comment_metadata.ElasticFieldName] = propertyName
		comment := columnMetadata.Marshall()

		result.WriteString(util.Indent(1))
		result.WriteString(fmt.Sprintf("\"%s\" %s '%s'", column.ClickHouseColumnName, column.ClickHouseType+" COMMENT ", comment))
	}
	return result.String()
}

func JsonToColumns(m SchemaMap, chConfig *clickhouse.ChTableConfig) []CreateTableEntry {
	var resultColumns []CreateTableEntry

	for name, value := range m {
		fType, err := clickhouse.NewType(value, name)
		if err != nil {
			// Skip column with invalid/incomplete type
			logger.Warn().Msgf("Skipping field '%s' with invalid/incomplete type: %v", name, err)
			continue
		}

		fTypeString := fType.String()
		if (!strings.Contains(fTypeString, "Array") && !strings.Contains(fTypeString, "Tuple")) && !strings.Contains(fTypeString, "DateTime") {
			fTypeString = "Nullable(" + fTypeString + ")"
		}

		// hack for now
		if name == timestampFieldName && chConfig.TimestampDefaultsNow {
			fTypeString += " DEFAULT now64()"
		}

		resultColumns = append(resultColumns, CreateTableEntry{ClickHouseColumnName: name, ClickHouseType: fTypeString})

	}
	return resultColumns
}

func SchemaToColumns(schemaMapping *schema.Schema, nameFormatter TableColumNameFormatter, tableName string, fieldEncodings map[schema.FieldEncodingKey]schema.EncodedFieldName) map[schema.FieldName]CreateTableEntry {
	resultColumns := make(map[schema.FieldName]CreateTableEntry)

	if schemaMapping == nil {
		return resultColumns
	}

	for _, field := range schemaMapping.Fields {
		var fType string
		internalPropertyName := string(fieldEncodings[schema.FieldEncodingKey{TableName: tableName, FieldName: field.PropertyName.AsString()}])
		switch field.Type.Name {
		default:
			logger.Warn().Msgf("Unsupported field type '%s' for field '%s' when trying to create a table. Ignoring that field.", field.Type.Name, field.PropertyName.AsString())
			continue
		case schema.QuesmaTypePoint.Name:
			lat := string(fieldEncodings[schema.FieldEncodingKey{TableName: tableName, FieldName: field.PropertyName.AsString() + ".lat"}])
			lon := string(fieldEncodings[schema.FieldEncodingKey{TableName: tableName, FieldName: field.PropertyName.AsString() + ".lon"}])
			if len(lat) == 0 || len(lon) == 0 {
				logger.Error().Msgf("Empty internal property names for geo_point field '%s' (lat: '%s'/lon: '%s'). This might result in incorrect table schema.", field.PropertyName.AsString(), lat, lon)
			}

			resultColumns[schema.FieldName(lat)] = CreateTableEntry{ClickHouseColumnName: lat, ClickHouseType: "Nullable(Float64)"}
			resultColumns[schema.FieldName(lon)] = CreateTableEntry{ClickHouseColumnName: lon, ClickHouseType: "Nullable(Float64)"}
			continue

		// Simple types:
		case schema.QuesmaTypeText.Name:
			fType = "Nullable(String)"
		case schema.QuesmaTypeKeyword.Name:
			fType = "Nullable(String)"
		case schema.QuesmaTypeLong.Name:
			fType = "Nullable(Int64)"
		case schema.QuesmaTypeUnsignedLong.Name:
			fType = "Nullable(Uint64)"
		case schema.QuesmaTypeTimestamp.Name:
			fType = "Nullable(DateTime64)"
		case schema.QuesmaTypeDate.Name:
			fType = "Nullable(Date)"
			// TODO: This (and Nullable(DateTime64) above) can be problematic for ingest when when set by user explicitly. We should either not use Nullable in this case
			// or add some validation logic so that its handled properly.
			// Example if someone sets `type: date` to a field in schemaOverrides AND this is a timestamp field for which we have dedicated logic (use DateTime64 + add DEFAULT now64())
			// Ingest will FAIL creating table with "Sorting key contains nullable columns, but merge tree setting `allow_nullable_key` is disabled"
		case schema.QuesmaTypeFloat.Name:
			fType = "Nullable(Float64)"
		case schema.QuesmaTypeBoolean.Name:
			fType = "Nullable(Bool)"
		}
		if len(internalPropertyName) == 0 {
			logger.Error().Msgf("Empty internal property name for field '%s'. This might result in incorrect table schema.", field.PropertyName.AsString())
		}
		resultColumns[schema.FieldName(internalPropertyName)] = CreateTableEntry{ClickHouseColumnName: internalPropertyName, ClickHouseType: fType}
	}
	return resultColumns
}

// Returns map with fields that are in 'sm', but not in our table schema 't'.
// Works with nested JSONs.
// Doesn't check any types of fields, only names.
func DifferenceMap(sm SchemaMap, t *clickhouse.Table) SchemaMap {
	mDiff := make(SchemaMap)
	var keysNested []string

	add := func(mapToAdd interface{}, name string) {
		mDiffCur := mDiff
		for _, key := range keysNested {
			_, ok := mDiffCur[key]
			if !ok {
				mDiffCur[key] = make(SchemaMap)
			}
			mDiffCur = mDiffCur[key].(SchemaMap)
		}
		mDiffCur[name] = mapToAdd
	}

	// 'schemaCol' isn't nil
	var descendRec func(mCur SchemaMap, schemaCol *clickhouse.Column)
	descendRec = func(mCur SchemaMap, schemaCol *clickhouse.Column) {
		// create a map of fields that exist in 'mCur', but not in 'schemaCol'
		// done in a way like below, because we want to iterate over columns,
		// which are an array, and not over mCur which is a map (faster this way)

		mvc, ok := schemaCol.Type.(clickhouse.MultiValueType)
		if !ok {
			// most likely case first
			if _, ok = mCur[schemaCol.Name]; ok {
				delete(mCur, schemaCol.Name)
			}
			if len(mCur) != 0 {
				add(mCur, schemaCol.Name)
			}
		} else {
			dontAddOnThisLvl := make(map[string]struct{})
			for _, col := range mvc.Cols {
				dontAddOnThisLvl[col.Name] = struct{}{}
				mNested, ok := mCur[col.Name]
				if ok {
					mNestedMap, ok := mNested.(SchemaMap)
					if ok {
						keysNested = append(keysNested, col.Name)
						descendRec(mNestedMap, col)
						keysNested = keysNested[:len(keysNested)-1]
					}
				}
			}
			for k, v := range mCur {
				if _, ok := dontAddOnThisLvl[k]; !ok {
					add(v, k)
				}
			}
		}
	}

	for name, v := range sm {
		col, ok := t.Cols[name]
		if !ok {
			mDiff[name] = v
		} else if mNested, ok := v.(SchemaMap); ok {
			keysNested = append(keysNested, name)
			descendRec(mNested, col)
			keysNested = keysNested[:len(keysNested)-1]
		}
	}
	return mDiff
}

// removes fields from 'm' that are not in 't'
func RemoveNonSchemaFields(m SchemaMap, t *clickhouse.Table) SchemaMap {
	var descendRec func(_ *clickhouse.Column, _ SchemaMap)
	descendRec = func(col *clickhouse.Column, mCur SchemaMap) {
		mvc, ok := col.Type.(clickhouse.MultiValueType)
		if !ok {
			return
		}

		dontRemoveOnThisLvl := make(map[string]struct{})
		for _, col := range mvc.Cols {
			dontRemoveOnThisLvl[col.Name] = struct{}{}
			mCurNested, ok := mCur[col.Name]
			if ok {
				mCurNestedMap, ok := mCurNested.(SchemaMap)
				if ok {
					descendRec(col, mCurNestedMap)
				}
			}
		}
		for fieldName, v := range mCur {
			if _, ok := dontRemoveOnThisLvl[fieldName]; !ok {
				delete(mCur, fieldName)
			}
			vCasted, ok := v.(SchemaMap)
			if ok && len(vCasted) == 0 {
				delete(mCur, fieldName)
			}
		}
		/*
			for fieldName, v := range mCur {
				mSchemaNested, ok := col.[fieldName]
				if !ok {
					delete(mCur, fieldName)
					continue
				} else {
					mSchemaNestedCasted, ok1 := mSchemaNested.(SchemaMap)
					mCurNested, ok2 := v.(SchemaMap)
					if ok1 && ok2 {
						descendRec(mSchemaNestedCasted, mCurNested, mCur)
					}
				}
				vCasted, ok := v.(SchemaMap)
				if ok && len(vCasted) == 0 {
					delete(mCur, fieldName)
				}
			}*/
	}

	for fieldName, v := range m {
		col, ok := t.Cols[fieldName]
		if ok {
			vCasted, ok := v.(SchemaMap)
			if ok {
				descendRec(col, vCasted)
			}
		} else {
			delete(m, fieldName) // TODO check if it's fine? In c++ not, but here seems to work
		}
	}
	return m
}

func BuildAttrsMap(m SchemaMap, config *clickhouse.ChTableConfig) (map[string][]interface{}, error) {
	result := make(map[string][]interface{}) // check if works
	for _, name := range sortedKeys(m) {
		value := m[name]
		matched := false
		for _, a := range config.Attributes {
			if a.Type.CanConvert(value) {
				result[a.KeysArrayName] = append(result[a.KeysArrayName], name)
				result[a.ValuesArrayName] = append(result[a.ValuesArrayName], fmt.Sprintf("%v", value))

				valueType, err := clickhouse.NewType(value, name)
				if err != nil {
					result[a.TypesArrayName] = append(result[a.TypesArrayName], clickhouse.UndefinedType)
				} else {
					result[a.TypesArrayName] = append(result[a.TypesArrayName], valueType.String())
				}

				matched = true
				break
			}
		}
		if !matched {
			return nil, fmt.Errorf("no attribute array matched for field %s", name)
		}
	}

	return result, nil
}

func sortedKeys(m SchemaMap) (keys []string) {
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)
	return keys
}
