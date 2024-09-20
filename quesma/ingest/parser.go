// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"fmt"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/schema"
	"quesma/util"
	"slices"
	"strings"
)

const NestedSeparator = "::"

type CreateTableEntry struct {
	ClickHouseColumnName string
	ClickHouseType       string
}

// Rendering columns to string
func columnsToString(columnsFromJson []CreateTableEntry,
	columnsFromSchema map[schema.FieldName]CreateTableEntry,
	fieldEncodings map[schema.FieldEncodingKey]schema.EncodedFieldName,
	tableName string,
) string {

	reverseFieldEncoding := make(map[schema.EncodedFieldName]schema.FieldEncodingKey)

	for k, v := range fieldEncodings {
		if k.TableName == tableName {
			reverseFieldEncoding[v] = k
		}
	}

	var result strings.Builder
	first := true
	for _, columnFromJson := range columnsFromJson {
		if first {
			first = false
		} else {
			result.WriteString(",\n")
		}
		result.WriteString(util.Indent(1))

		if columnFromSchema, found := columnsFromSchema[schema.FieldName(columnFromJson.ClickHouseColumnName)]; found && !strings.Contains(columnFromJson.ClickHouseType, "Array") {
			// Schema takes precedence over JSON (except for Arrays which are not currently handled)
			result.WriteString(fmt.Sprintf("\"%s\" %s '%s'", columnFromSchema.ClickHouseColumnName, columnFromSchema.ClickHouseType+" COMMENT ", reverseFieldEncoding[schema.EncodedFieldName(columnFromSchema.ClickHouseColumnName)].FieldName))
		} else {
			result.WriteString(fmt.Sprintf("\"%s\" %s '%s'", columnFromJson.ClickHouseColumnName, columnFromJson.ClickHouseType+" COMMENT ", reverseFieldEncoding[schema.EncodedFieldName(columnFromJson.ClickHouseColumnName)].FieldName))
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
		result.WriteString(util.Indent(1))
		result.WriteString(fmt.Sprintf("\"%s\" %s '%s'", column.ClickHouseColumnName, column.ClickHouseType+" COMMENT ", reverseFieldEncoding[schema.EncodedFieldName(column.ClickHouseColumnName)].FieldName))
	}
	return result.String()
}

func JsonToColumns(namespace string, m SchemaMap, indentLvl int, chConfig *clickhouse.ChTableConfig, nameFormatter TableColumNameFormatter, ignoredFields []config.FieldName) []CreateTableEntry {
	var resultColumns []CreateTableEntry

	for name, value := range m {
		listValue, isListValue := value.([]interface{})
		if isListValue {
			value = listValue
		}
		nestedValue, ok := value.(SchemaMap)
		if (ok && nestedValue != nil && len(nestedValue) > 0) && !isListValue {
			nested := JsonToColumns(nameFormatter.Format(namespace, name), nestedValue, indentLvl, chConfig, nameFormatter, ignoredFields)
			resultColumns = append(resultColumns, nested...)
		} else {
			var fTypeString string
			if value == nil { // HACK ALERT -> We're treating null values as strings for now, so that we don't completely discard documents with empty values
				fTypeString = "Nullable(String)"
			} else {
				fType := clickhouse.NewType(value)

				// handle "field":{} case (Elastic Agent sends such JSON fields) by ignoring them
				if multiValueType, ok := fType.(clickhouse.MultiValueType); ok && len(multiValueType.Cols) == 0 {
					logger.Warn().Msgf("Ignoring empty JSON object: \"%s\":%v (in %s)", name, value, namespace)
					continue
				}

				fTypeString = fType.String()
				if !strings.Contains(fTypeString, "Array") && !strings.Contains(fTypeString, "DateTime") {
					fTypeString = "Nullable(" + fTypeString + ")"
				}
			}
			// hack for now
			if indentLvl == 1 && name == timestampFieldName && chConfig.TimestampDefaultsNow {
				fTypeString += " DEFAULT now64()"
			}
			// We still may have name like:
			// "service.name": { "very.name": "value" }
			// Before that code it would be transformed to:
			// "service.name::very.name"
			// So I convert it to:
			// "service::name::very::name"

			internalName := nameFormatter.Format(namespace, name)
			resultColumns = append(resultColumns, CreateTableEntry{ClickHouseColumnName: internalName, ClickHouseType: fTypeString})
		}
	}
	return resultColumns
}

func SchemaToColumns(schemaMapping *schema.Schema, nameFormatter TableColumNameFormatter) map[schema.FieldName]CreateTableEntry {
	resultColumns := make(map[schema.FieldName]CreateTableEntry)

	if schemaMapping == nil {
		return resultColumns
	}

	for _, field := range schemaMapping.Fields {
		var fType string
		// TODO we are using util.FieldToColumnEncoder directly here
		// due to the fact that field encodings map is part of schema.Registry
		// not schema.Schema and we don't have access to it here.
		internalPropertyName := util.FieldToColumnEncoder(field.InternalPropertyName.AsString())
		switch field.Type.Name {
		default:
			logger.Warn().Msgf("Unsupported field type '%s' for field '%s' when trying to create a table. Ignoring that field.", field.Type.Name, field.PropertyName.AsString())
			continue
		case schema.QuesmaTypePoint.Name:
			lat := nameFormatter.Format(internalPropertyName, "lat")
			lon := nameFormatter.Format(internalPropertyName, "lon")
			resultColumns[schema.FieldName(lat)] = CreateTableEntry{ClickHouseColumnName: lat, ClickHouseType: "Nullable(String)"}
			resultColumns[schema.FieldName(lon)] = CreateTableEntry{ClickHouseColumnName: lon, ClickHouseType: "Nullable(String)"}
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
				result[a.TypesArrayName] = append(result[a.TypesArrayName], clickhouse.NewType(value).String())

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
