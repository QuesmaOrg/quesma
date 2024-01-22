package clickhouse

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

const nestedSeparator = "::"

// TODO remove schemamap type?
// TODO change all return types to * when worth it like here
func JsonToFieldsMap(jsonn string) (SchemaMap, error) {
	m := make(SchemaMap)
	err := json.Unmarshal([]byte(jsonn), &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func JsonToTableSchema(jsonn, tableName string, config *ChTableConfig) (*Table, error) {
	m, err := JsonToFieldsMap(jsonn)
	if err != nil {
		return nil, err
	}

	cols := make(map[string]*Column)
	for name, value := range m {
		// TODO make fields private and add constructor?
		cols[name] = &Column{Name: name, Type: NewType(value), Codec: Codec{Name: ""}} // TODO codec not supported
	}

	// add others
	if _, ok := cols[othersFieldName]; config.hasOthers && !ok {
		cols[othersFieldName] = &Column{Name: othersFieldName, Type: NewType(SchemaMap{}), Codec: Codec{Name: ""}} // TODO codec not supported
	}
	// add attributes
	for _, a := range config.attributes {
		if _, ok := cols[a.KeysArrayName]; !ok {
			cols[a.KeysArrayName] = &Column{Name: a.KeysArrayName, Type: NewType(""), Codec: Codec{Name: ""}} // TODO codec not supported
		}
		if _, ok := cols[a.ValuesArrayName]; !ok {
			cols[a.ValuesArrayName] = &Column{Name: a.ValuesArrayName, Type: a.Type, Codec: Codec{Name: ""}} // TODO codec not supported
		}
	}

	return &Table{Name: tableName, Config: config, Cols: cols}, nil
}

// m: unmarshalled json from HTTP request
// Returns nicely formatted string for CREATE TABLE command
func FieldsMapToCreateTableString(namespace string, m SchemaMap, indentLvl int, config *ChTableConfig) string {
	var result strings.Builder
	i := 0
	for name, value := range m {
		if namespace == "" {
			result.WriteString("\n")
		}
		nestedValue, ok := value.(SchemaMap)
		if ok && nestedValue != nil && len(nestedValue) > 0 {
			var nested []string
			if namespace == "" {
				nested = append(nested, FieldsMapToCreateTableString(name, nestedValue, indentLvl, config))
			} else {
				nested = append(nested, FieldsMapToCreateTableString(fmt.Sprintf("%s%s%s", namespace, nestedSeparator, name), nestedValue, indentLvl, config))
			}

			result.WriteString(strings.Join(nested, ",\n"))
		} else {
			// value is a single field. Only String/Bool/DateTime64 supported for now.
			fType := NewType(value).String()
			// hack for now
			if indentLvl == 1 && name == timestampFieldName && config.timestampDefaultsNow {
				fType += " DEFAULT now64()"
			}
			result.WriteString(indent(indentLvl))
			if namespace == "" {
				result.WriteString(fmt.Sprintf("\"%s\" %s", name, fType))
			} else {
				result.WriteString(fmt.Sprintf("\"%s%s%s\" %s", namespace, nestedSeparator, name, fType))
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
	/*
		m := SchemaMap{
			"host_name": SchemaMap{
				"a": SchemaMap{
					"b": nil,
				},
				"b": nil,
			},
			"message":      nil,
			"service_name": nil,
			"severity":     nil,
			"source":       nil,
			"timestamp":    nil,
			"non-schema":   nil,
		}
		table := &Table{
			Cols: map[string]*Column{
				"host_name": {Name: "host_name", Codec: Codec{Name: ""}, Type: MultiValueType{
					Name: "Tuple", Cols: []*Column{
						{Name: "b", Type: NewBaseType("String")},
					},
				}},
				"message":      nil,
				"service_name": nil,
				"severity":     nil,
				"timestamp":    nil,
				"source":       nil,
			},
		}*/

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

// removes fields from 'm' that are not in 't'
func RemoveNonSchemaFields(m SchemaMap, t *Table) SchemaMap {
	var descendRec func(_ *Column, _, _ SchemaMap)
	descendRec = func(col *Column, mCur, mCurParent SchemaMap) {
		mvc, ok := col.Type.(MultiValueType)
		if !ok {
			// most likely case first
			mCurParent[col.Name] = nil // that might be wrong, but in tests it seems fine
			// some less trivial handling might be needed
			return
		}

		dontRemoveOnThisLvl := make(map[string]struct{})
		for _, col := range mvc.Cols {
			dontRemoveOnThisLvl[col.Name] = struct{}{}
			mCurNested, ok := mCur[col.Name]
			if ok {
				mCurNestedMap, ok := mCurNested.(SchemaMap)
				if ok {
					descendRec(col, mCurNestedMap, mCur)
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
				descendRec(col, vCasted, m)
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
				result[a.ValuesArrayName] = append(result[a.ValuesArrayName], value)
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
