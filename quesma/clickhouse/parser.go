// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"fmt"
	"quesma/plugins"
	"quesma/schema"
	"quesma/util"
	"slices"
	"strings"
)

const NestedSeparator = "::"

// m: unmarshalled json from HTTP request
// Returns nicely formatted string for CREATE TABLE command
func FieldsMapToCreateTableString(namespace string, m SchemaMap, indentLvl int, config *ChTableConfig, nameFormatter plugins.TableColumNameFormatter, schemaMapping *schema.Schema) string {

	var result strings.Builder
	i := 0
	for name, value := range m {
		if namespace == "" {
			result.WriteString("\n")
		}

		listValue, isListValue := value.([]interface{})
		if isListValue {
			value = listValue
		}
		nestedValue, ok := value.(SchemaMap)
		if (ok && nestedValue != nil && len(nestedValue) > 0) && !isListValue {
			var nested []string
			if namespace == "" {
				nested = append(nested, FieldsMapToCreateTableString(name, nestedValue, indentLvl, config, nameFormatter, nil))
			} else {
				nested = append(nested, FieldsMapToCreateTableString(nameFormatter.Format(namespace, name), nestedValue, indentLvl, config, nameFormatter, nil))
			}

			result.WriteString(strings.Join(nested, ",\n"))
		} else {
			// value is a single field. Only String/Bool/DateTime64 supported for now.
			var fType string
			if value == nil { // HACK ALERT -> We're treating null values as strings for now, so that we don't completely discard documents with empty values
				fType = "Nullable(String)"
			} else {
				fType = NewType(value).String()
				if !strings.Contains(fType, "Array") && !strings.Contains(fType, "DateTime") {
					fType = "Nullable(" + fType + ")"
				}
			}
			// hack for now
			if indentLvl == 1 && name == timestampFieldName && config.timestampDefaultsNow {
				fType += " DEFAULT now64()"
			}
			result.WriteString(util.Indent(indentLvl))
			if namespace == "" {
				result.WriteString(fmt.Sprintf("\"%s\" %s", name, fType))
			} else {
				result.WriteString(fmt.Sprintf("\"%s\" %s", nameFormatter.Format(namespace, name), fType))
			}
		}
		if i+1 < len(m) {
			result.WriteString(",")
		}

		if namespace != "" && i+1 < len(m) {
			result.WriteString("\n")
		}

		i++
	}
	return result.String()
}

// Returns map with fields that are in 'sm', but not in our table schema 't'.
// Works with nested JSONs.
// Doesn't check any types of fields, only names.
func DifferenceMap(sm SchemaMap, t *Table) SchemaMap {
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
	var descendRec func(mCur SchemaMap, schemaCol *Column)
	descendRec = func(mCur SchemaMap, schemaCol *Column) {
		// create a map of fields that exist in 'mCur', but not in 'schemaCol'
		// done in a way like below, because we want to iterate over columns,
		// which are an array, and not over mCur which is a map (faster this way)

		mvc, ok := schemaCol.Type.(MultiValueType)
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

func RemoveTypeMismatchSchemaFields(m SchemaMap, t *Table) SchemaMap {
	handleType := func(col *Column, schema SchemaMap, value interface{}) {
		kind, err := util.KindFromString(col.Type.String())
		// All numbers in json are by default float type
		// We don't want to filter out those that
		// have empty decimal part
		// "!isFloat64" -> to make e.g. string column with integer value fail, as it would in the actual insert
		_, isFloat64 := value.(float64)
		if err == nil && util.ValueKind(value) != kind && (!isFloat64 || !util.IsInt(value)) {
			delete(schema, col.Name)
		}
	}
	var descendRec func(_ *Column, _ SchemaMap)
	descendRec = func(col *Column, mCur SchemaMap) {
		switch columnType := col.Type.(type) {
		case BaseType:
			value := mCur[col.Name]
			handleType(col, mCur, value)
		case CompoundType:
			multi, ok := columnType.BaseType.(MultiValueType)
			if !ok {
				return
			}
			for _, col := range multi.Cols {
				value, ok := mCur[col.Name]
				if ok {
					handleType(col, mCur, value)
					mCurNestedMap, ok := value.(SchemaMap)
					if ok {
						descendRec(col, mCurNestedMap)
					}
				}
			}
		case MultiValueType:
			for _, col := range columnType.Cols {
				value, ok := mCur[col.Name]
				if ok {
					handleType(col, mCur, value)
					mCurNestedMap, ok := value.(SchemaMap)
					if ok {
						descendRec(col, mCurNestedMap)
					}
				}
			}
		}
	}
	for fieldName, v := range m {
		col, ok := t.Cols[fieldName]
		if ok && col != nil {
			switch v := v.(type) {
			case SchemaMap:
				descendRec(col, v)

			case []interface{}:
				for _, arrayElement := range v {
					vCasted, ok := arrayElement.(SchemaMap)
					if ok {
						descendRec(col, vCasted)
					} else {
						innerType, ok := col.Type.(CompoundType)
						if ok {
							handleType(col, m, arrayElement)
							kind, err := util.KindFromString(innerType.BaseType.String())
							valueKind := util.ValueKind(arrayElement)
							if err == nil && valueKind != kind {
								delete(m, fieldName)
							}
						}
					}
				}
			case interface{}:
				_, ok := col.Type.(BaseType)
				if ok {
					handleType(col, m, v)
				}
			}
		}
	}
	return m
}

// removes fields from 'm' that are not in 't'
func RemoveNonSchemaFields(m SchemaMap, t *Table) SchemaMap {
	var descendRec func(_ *Column, _ SchemaMap)
	descendRec = func(col *Column, mCur SchemaMap) {
		mvc, ok := col.Type.(MultiValueType)
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

func BuildAttrsMapAndOthers(m SchemaMap, config *ChTableConfig) (map[string][]interface{}, SchemaMap, error) {
	result := make(map[string][]interface{}) // check if works
	others := make(SchemaMap)
	for _, name := range sortedKeys(m) {
		value := m[name]
		matched := false
		for _, a := range config.attributes {
			if a.Type.canConvert(value) {
				result[a.KeysArrayName] = append(result[a.KeysArrayName], name)
				result[a.ValuesArrayName] = append(result[a.ValuesArrayName], fmt.Sprintf("%v", value))
				matched = true
				break
			}
		}
		if !matched {
			if config.hasOthers {
				others[name] = value
			} else {
				return nil, nil, fmt.Errorf("no attribute array matched for field %s", name)
			}
		}
	}

	return result, others, nil
}

func sortedKeys(m SchemaMap) (keys []string) {
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)
	return keys
}
