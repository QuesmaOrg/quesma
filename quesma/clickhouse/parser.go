package clickhouse

import (
	"encoding/json"
	"fmt"
	"strings"
)

func JsonToFieldsMap(jsonn string) (SchemaMap, error) {
	m := make(SchemaMap)
	err := json.Unmarshal([]byte(jsonn), &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// m: unmarshalled json from HTTP request
// Returns nicely formatted string for CREATE TABLE command
func FieldsMapToCreateTableString(m SchemaMap, indentLvl int, config ChTableConfig) string {
	var result strings.Builder
	i := 0
	for name, value := range m {
		result.WriteString(indent(indentLvl))
		nestedValue, ok := value.(SchemaMap)
		if name == "create" {
			fmt.Println("tutaj! ", ok, nestedValue)
		}
		if ok && nestedValue != nil && len(nestedValue) > 0 { // value is another (nested) dict
			// Care. Empty JSON fields will be replaced by String.
			// So far, it's better as all JSONs we treat as Tuples, and empty Tuple
			// is an error in Clickhouse.
			// But for the future, we might want to change that.

			// quotes near field names very important. Normally they are not, but
			// they enable to have fields with reserved names, like e.g. index.
			result.WriteString(fmt.Sprintf("\"%s\" Tuple\n%s(\n%s%s)", name,
				indent(indentLvl), FieldsMapToCreateTableString(nestedValue, indentLvl+1, config), indent(indentLvl)))
		} else {
			// value is a single field. Only String/Bool/DateTime64 supported for now.
			fType := determineFieldType(value)
			// hack for now
			if indentLvl == 1 && name == timestampFieldName && config.timestampDefaultsNow {
				fType += " DEFAULT now64()"
			}
			result.WriteString(fmt.Sprintf("\"%s\" %s", name, fType))
		}
		if i+1 < len(m) {
			result.WriteString(",")
		}
		i++
		result.WriteString("\n")
	}
	return result.String()
}

// returns map with fields that are in `m`, but not in `mExpected`
func DifferenceMap(mExpected, m SchemaMap) SchemaMap {
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

	var descendRec func(_, _ SchemaMap)
	descendRec = func(mCur, mExpectedCur SchemaMap) {
		for name, v := range mCur {
			vExpectedMap, ok := mExpectedCur[name]
			if !ok {
				add(mCur[name], name)
			} else {
				vMap, ok := v.(SchemaMap)
				if !ok {
					continue
				}
				keysNested = append(keysNested, name)
				descendRec(vMap, vExpectedMap.(SchemaMap))
				keysNested = keysNested[:len(keysNested)-1]
			}
		}
	}

	for name, v := range m {
		mExpectedNested, ok := mExpected[name]
		if !ok {
			mDiff[name] = v
		} else if mNested, ok := m[name].(SchemaMap); ok {
			keysNested = append(keysNested, name)
			descendRec(mNested, mExpectedNested.(SchemaMap))
			keysNested = keysNested[:len(keysNested)-1]
		}
	}
	return mDiff
}

// removes fields from 'm' that are not in 'mSchema'
func RemoveNonSchemaFields(mSchema, m SchemaMap) SchemaMap {
	var descendRec func(_, _, _ SchemaMap)
	descendRec = func(mSchemaCur, mCur, mCurParent SchemaMap) {
		for fieldName, v := range mCur {
			mSchemaNested, ok := mSchemaCur[fieldName]
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
		}
	}

	for fieldName, v := range m {
		mSchemaNested, ok := mSchema[fieldName]
		if ok {
			vCasted, ok := v.(SchemaMap)
			if ok {
				// no need for cast check, as 'mSchema' values are always
				// either SchemaMap, or nil
				descendRec(mSchemaNested.(SchemaMap), vCasted, m)
			}
		} else {
			delete(m, fieldName)
		}
	}
	return m
}
