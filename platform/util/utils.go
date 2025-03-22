// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/base64"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/goccy/go-json"
	"github.com/hashicorp/go-multierror"
	"github.com/k0kubun/pp"
	"io"
	"log"
	"net/http"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type JsonMap = map[string]interface{}

func prettify(jsonStr string) string {
	data := []byte(jsonStr)
	empty := []byte{}
	buf := bytes.NewBuffer(empty)
	err := json.Indent(buf, data, "", "  ")
	if err != nil {
		panic(err)
	}
	readBuf, _ := io.ReadAll(buf)
	return string(readBuf)
}

func Shorten(body interface{}) interface{} {
	switch bodyType := body.(type) {
	case map[string]interface{}:
		for k, nested := range bodyType {
			bodyType[k] = Shorten(nested)
		}
	case []interface{}:
		if len(bodyType) > 3 {
			t := bodyType[:3]
			t[2] = "..."
			return t
		}
	}
	return body
}

func JsonPrettify(jsonStr string, shorten bool) string {
	var jsonData map[string]interface{}

	err := json.Unmarshal([]byte(jsonStr), &jsonData)
	if err != nil {
		return fmt.Sprintf("Error unmarshalling JSON: %v, json: %s", err, jsonStr)
	}

	for k, nested := range jsonData {
		if shorten {
			jsonData[k] = Shorten(nested)
		}
	}
	v, err := json.Marshal(jsonData)
	if err != nil {
		return fmt.Sprintf("Error marshalling JSON: %v, json: %s", err, jsonStr)
	}
	return prettify(string(v))
}

func JsonToMap(jsonn string) (JsonMap, error) {
	m := make(JsonMap)
	err := json.Unmarshal([]byte(jsonn), &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// MapDifference returns pair of maps with fields that are present in one of input maps and not in the other
// specifically (mActual - mExpected, mExpected - mActual)
// * mActual - uses JsonMap fully: values are []JsonMap, or JsonMap, or base types
// * mExpected - value can also be []any, because it's generated from Golang's json.Unmarshal
// * acceptableDifference - list of keys that are allowed to be different
// * compareBaseTypes - if true, we compare base type values as well (e.g. if mActual["key1"]["key2"] == 1,
// and mExpected["key1"]["key2"] == 2, we say that they are different)
// * compareFullArrays - if true, we compare entire arrays, if false just first element ([0])
// FIXME all tests are with acceptableDifference = [], add some else
func MapDifference(mActual, mExpected JsonMap, acceptableDifference []string,
	compareBaseTypes, compareFullArrays bool) (JsonMap, JsonMap) {

	// We're adding 'mapToAdd' to 'resultDiff' at the key keysNested + name (`keysNested` is a list of keys, as JSONs can be nested)
	// (or before, if such map doesn't exist on previous nested levels)
	// append(keysNested, name) - list of keys to get to the current map ('mapToAdd')
	addToResult := func(mapToAdd interface{}, name string, keysNested []string, resultDiff JsonMap) {
		cur := resultDiff
		for _, key := range keysNested {
			_, ok := cur[key]
			if !ok {
				cur[key] = make(JsonMap)
			}
			cur = cur[key].(JsonMap)
		}
		cur[name] = mapToAdd
	}

	var descendRec func(_, _, _, _ JsonMap, _ []string)

	// 1. Add values that are present in 'mActualCur' but not in 'mExpectedCur' to 'resultDiffThis'
	// 2. Call 'descendRec' for values that are present in both maps (if 'descendFurther' == true)
	// 'descendFurther' - whether to descend further into nested maps/arrays.
	// Introduced, because at each level we invoke this function twice, and we want to descend only once.
	// 'keysNested' - backtrack slice, keeps keys to get to the current level of nested maps 'mActualCur' and 'mExpectedCur'
	compareCurrentLevelAndDescendRec := func(mActualCur, mExpectedCur JsonMap, resultDiffThis, resultDiffOther JsonMap, keysNested []string, descendFurther bool) {
		for name, vActual := range mActualCur {
			vExpected, ok := mExpectedCur[name]
			if !ok {
				if !slices.Contains(acceptableDifference, name) {
					addToResult(mActualCur[name], name, keysNested, resultDiffThis)
				}
				// FIXME maybe check if vActual is a base type. If it's not, we probably should add it to resultDiff.
				continue
			}
			if !descendFurther {
				// here we know value is present in both maps, but if we don't descend, we just continue
				continue
			}
			switch vActualTyped := vActual.(type) {
			case JsonMap:
				vExpectedMap, ok := vExpected.(JsonMap)
				if !ok {
					addToResult(vActualTyped, name, keysNested, resultDiffThis)
					if vExpected != nil {
						addToResult(vExpected, name, keysNested, resultDiffOther)
					}
					continue
				}
				keysNested = append(keysNested, name)
				descendRec(vActualTyped, vExpectedMap, resultDiffThis, resultDiffOther, keysNested)
				keysNested = keysNested[:len(keysNested)-1]
			case []JsonMap:
				vExpectedArr, ok := vExpected.([]any)
				if !ok {
					// might be a bit slower, casting all JsonMaps to []any, but it's simpler this way. Change if it becomes a bottleneck.
					vExpectedArr = make([]any, 0)
					if vExpectedAsJsonMap, ok := vExpected.([]JsonMap); ok {
						for _, val := range vExpectedAsJsonMap {
							vExpectedArr = append(vExpectedArr, val)
						}
					} else {
						// Just a safe check. If they are different types, we add to both results. Can be changed, but they shouldn't be different anyway.
						addToResult(vActualTyped, name, keysNested, resultDiffThis)
						addToResult(vExpected, name, keysNested, resultDiffOther)
						continue
					}
				}

				lenActual, lenExpected := len(vActualTyped), len(vExpectedArr)
				if !compareFullArrays {
					lenActual, lenExpected = min(1, lenActual), min(1, lenExpected)
				}
				for i := 0; i < min(lenActual, lenExpected); i++ {
					// Code below doesn't cover 100% of cases. But covers all we see, so I don't want to
					// waste time improving it until there's need for it.
					// Assumption: elements of arrays are maps or base types. From observations, it's always true.
					// better to assume that until it breaks at least once. Fixing would require a lot of new code.
					expectedArrElementAsMap, ok := vExpectedArr[i].(JsonMap)
					if ok {
						keysNested = append(keysNested, name+"["+strconv.Itoa(i)+"]")
						descendRec(vActualTyped[i], expectedArrElementAsMap, resultDiffThis, resultDiffOther, keysNested)
						keysNested = keysNested[:len(keysNested)-1]
					} else {
						addToResult(vActualTyped[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffThis)
					}
				}
				for i := min(lenActual, lenExpected); i < lenActual; i++ {
					addToResult(vActualTyped[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffThis)
				}
				for i := min(lenActual, lenExpected); i < lenExpected; i++ {
					addToResult(vExpectedArr[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffOther)
				}
			case []any:
				vExpectedArr, ok := vExpected.([]any)
				if !ok {
					addToResult(vActualTyped, name, keysNested, resultDiffThis)
					addToResult(vExpected, name, keysNested, resultDiffOther)
					continue
				}
				lenActual, lenExpected := len(vActualTyped), len(vExpectedArr)
				for i := 0; i < min(lenActual, lenExpected); i++ {
					// Code below doesn't cover the case, when elements of arrays are subarrays.
					// But it should return that they are different, so it should be fine - we should notice that.
					actualArrElementAsMap, okActualAsMap := vActualTyped[i].(JsonMap)
					expectedArrElementAsMap, okExpectedAsMap := vExpectedArr[i].(JsonMap)
					if okActualAsMap && okExpectedAsMap {
						keysNested = append(keysNested, name+"["+strconv.Itoa(i)+"]")
						descendRec(actualArrElementAsMap, expectedArrElementAsMap, resultDiffThis, resultDiffOther, keysNested)
						keysNested = keysNested[:len(keysNested)-1]
					} else if !okActualAsMap && !okExpectedAsMap {
						if compareBaseTypes && !equal(vActualTyped[i], vExpectedArr[i]) {
							addToResult(vActualTyped[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffThis)
							addToResult(vExpectedArr[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffOther)
						}
					} else {
						addToResult(vActualTyped[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffThis)
						addToResult(vExpectedArr[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffOther)
					}
				}
				for i := min(lenActual, lenExpected); i < lenActual; i++ {
					addToResult(vActualTyped[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffThis)
				}
				for i := min(lenActual, lenExpected); i < lenExpected; i++ {
					addToResult(vExpectedArr[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffOther)
				}
			default:
				if !equal(vActual, vExpected) && compareBaseTypes && !slices.Contains(acceptableDifference, name) {
					// FIXME maybe check if both vActual && vExpected are indeed base types.
					// If they are not, we probably should add them to both results, even if acceptableDifference contains name.
					addToResult(vActual, name, keysNested, resultDiffThis)
					addToResult(vExpected, name, keysNested, resultDiffOther)
				}
			}
		}
	}

	// 'keysNested' - backtrack slice, keeps keys to get to the current level of nested maps 'mCur' and 'mExpectedCur'
	descendRec = func(mCur, mExpectedCur, resultDiff1, resultDiff2 JsonMap, keysNested []string) {
		compareCurrentLevelAndDescendRec(mCur, mExpectedCur, resultDiff1, resultDiff2, keysNested, true)
		compareCurrentLevelAndDescendRec(mExpectedCur, mCur, resultDiff2, resultDiff1, keysNested, false)
	}

	mDiffActualMinusExpected, mDiffExpectedMinusActual := make(JsonMap), make(JsonMap)
	var keysNested []string
	descendRec(mActual, mExpected, mDiffActualMinusExpected, mDiffExpectedMinusActual, keysNested)
	return mDiffActualMinusExpected, mDiffExpectedMinusActual
}

// returns pair of maps with fields that are present in one of input JSONs and not in the other
// specifically (jsonActual - jsonExpected, jsonExpected - jsonActual, err)
func JsonDifference(jsonActual, jsonExpected string) (JsonMap, JsonMap, error) {
	mActual, err := JsonToMap(jsonActual)
	if err != nil {
		return nil, nil, fmt.Errorf("%v (first JSON, json: %s)", err, jsonActual)
	}
	mExpected, err := JsonToMap(jsonExpected)
	if err != nil {
		return nil, nil, fmt.Errorf("%v (second JSON, json: %s)", err, jsonExpected)
	}
	actualMinusExpected, expectedMinusActual := MapDifference(mActual, mExpected, []string{}, false, false)
	return actualMinusExpected, expectedMinusActual, nil
}

// MergeMaps
// If there's type conflict, e.g. one map has {"a": map}, and second has {"a": array}, we log an error.
// Tried https://stackoverflow.com/a/71545414 and https://stackoverflow.com/a/71652767
// but none of them works for nested maps, so needed to write our own.
// * mActual - uses JsonMap fully: values are []JsonMap, or JsonMap, or base types
// * mExpected - value can also be []any, because it's generated from Golang's json.Unmarshal
func MergeMaps(mActual, mExpected JsonMap) (JsonMap, error) {
	var mergeMapsRec func(m1, m2 JsonMap) (JsonMap, error)

	// merges 'i1' and 'i2' in 3 cases: both are JsonMap, both are []JsonMap, or both are some base type
	mergeAny := func(i1, i2 any) (any, error) {
		var err *multierror.Error
		switch i1Typed := i1.(type) {
		case JsonMap:
			i2Typed, ok := i2.(JsonMap)
			if !ok {
				err = multierror.Append(err, fmt.Errorf("mergeAny: i1 is map, i2 is not. i1: %v, i2: %v", i1, i2))
				return i1, err
			}
			return mergeMapsRec(i1Typed, i2Typed)
		case []JsonMap:
			i2Typed, ok := i2.([]any)
			if !ok {
				// might be a bit slower, casting all JsonMaps to []any, but it's simpler this way. Change if it becomes a bottleneck.
				i2Typed = make([]any, 0)
				if i2AsJsonMap, ok := i2.([]JsonMap); ok {
					for _, val := range i2AsJsonMap {
						i2Typed = append(i2Typed, val)
					}
				} else {
					err = multierror.Append(err, fmt.Errorf("mergeAny: i1 is []JsonMap, i2 is not an array. i1: %v, i2: %v", i1Typed, i2))
				}
			}

			i1Len, i2Len := len(i1Typed), len(i2Typed)
			mergedArray := make([]JsonMap, 0, max(i1Len, i2Len))

			// CARE: keeps the old implementation for now, which seemed to work fine everywhere,
			// until one `sample_flights` dashboard. It's not perfect. TODO improve it.
			if i1Len == i2Len {
				for i := 0; i < i1Len; i++ {
					mergeRes, errMerge := mergeMapsRec(i1Typed[i], i2Typed[i].(JsonMap))
					err = multierror.Append(err, errMerge)
					mergedArray = append(mergedArray, mergeRes)
				}
				return mergedArray, err.ErrorOrNil()
			}

			i, j := 0, 0
			for i < i1Len && j < i2Len {
				var key1, key2 string
				key1, ok = i1Typed[i]["key"].(string) // TODO maybe some other types as well?
				if !ok {
					if key1Int, ok := i1Typed[i]["key"].(int64); ok {
						key1 = strconv.FormatInt(key1Int, 10)
					} else if key1Uint, ok := i1Typed[i]["key"].(uint64); ok {
						key1 = strconv.FormatUint(key1Uint, 10)
					} else if key1Float, ok := i1Typed[i]["key"].(float64); ok {
						key1 = strconv.FormatFloat(key1Float, 'f', -1, 64)
					} else {
						// TODO keys probably can be other types, e.g. bools
						err = multierror.Append(err, fmt.Errorf("mergeAny: key not found in i1: %v", i1Typed[i]))
						i += 1
						continue
					}
				}
				key2, ok = i2Typed[j].(JsonMap)["key"].(string) // TODO maybe some other types as well?
				if !ok {
					if key2Int, ok := i2Typed[j].(JsonMap)["key"].(int64); ok {
						key2 = strconv.FormatInt(key2Int, 10)
					} else if key2Uint, ok := i2Typed[j].(JsonMap)["key"].(uint64); ok {
						key2 = strconv.FormatUint(key2Uint, 10)
					} else if key2Float, ok := i2Typed[j].(JsonMap)["key"].(float64); ok {
						key2 = strconv.FormatFloat(key2Float, 'f', -1, 64)
					} else {
						// TODO keys probably can be other types, e.g. bools
						err = multierror.Append(err, fmt.Errorf("mergeAny: key not found in i2: %v", i2Typed[j]))
						j += 1
						continue
					}
				}
				if key1 == key2 {
					mergeResult, errMerge := mergeMapsRec(i1Typed[i], i2Typed[j].(JsonMap))
					err = multierror.Append(err, errMerge)
					mergedArray = append(mergedArray, mergeResult)
					i += 1
					j += 1
				} else if key1 < key2 {
					mergedArray = append(mergedArray, i1Typed[i])
					i += 1
				} else {
					mergedArray = append(mergedArray, i2Typed[j].(JsonMap))
					j += 1
				}
			}
			for i < i1Len {
				mergedArray = append(mergedArray, i1Typed[i])
				i += 1
			}
			for j < i2Len {
				mergedArray = append(mergedArray, i2Typed[j].(JsonMap))
				j += 1
			}

			return mergedArray, err.ErrorOrNil()

		default:
			if !reflect.DeepEqual(i1, i2) {
				err = multierror.Append(err, fmt.Errorf("mergeAny: i1 isn't neither JsonMap nor []JsonMap, i1 type: %T, i2 type: %T, i1: %v, i2: %v", i1, i2, i1, i2))
			}
			return i1, err.ErrorOrNil()
		}
	}

	mergeMapsRec = func(m1, m2 JsonMap) (JsonMap, error) {
		var err error
		mergedMap := make(JsonMap)
		for k, v1 := range m1 {
			v2, ok := m2[k]
			if ok {
				mergedMap[k], err = mergeAny(v1, v2)
			} else {
				mergedMap[k] = v1
			}
		}
		for k, v2 := range m2 {
			_, ok := m1[k]
			if !ok {
				mergedMap[k] = v2
			}
		}
		return mergedMap, err
	}
	return mergeMapsRec(mActual, mExpected)
}

func BodyHandler(h func(body []byte, writer http.ResponseWriter, r *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(writer http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		r.Body = io.NopCloser(bytes.NewBuffer(body))
		h(body, writer, r)
	}
}

// returns a slice with non-empty strings from input slice (in place of input slice)
func FilterNonEmpty(slice []string) []string {
	i := 0
	for _, el := range slice {
		if len(el) > 0 {
			slice[i] = el
			i++
		}
	}
	return slice[:i]
}

// Compares 2 strings for SQL-like equality, which is a bit looser than normal strings ==.
// E.g. "some-prefix A OR B some-suffix" == (SQL-like) "some-prefix B OR A some-suffix".
// It's useful in tests.
// This implementation is not correct in general case, it can return that some more complex
// strings aren't SQL-like equal, when they are, but it's good enough for our simple tests.
// (e.g. it only tries to find permutations of size 2)
func AssertSqlEqual(t *testing.T, expected, actual string) {
	if !IsSqlEqual(expected, actual) {
		pp.Println("-- Expected:")
		fmt.Printf("%s\n", SqlPrettyPrint([]byte(expected)))
		pp.Println("---- Actual:")
		fmt.Printf("%s\n", SqlPrettyPrint([]byte(actual)))
		actualLines := strings.Split(actual, "\n")
		expectedLines := strings.Split(expected, "\n")
		pp.Println("-- First diff: ")
		for i, aLine := range actualLines {
			if i >= len(expectedLines) {
				fmt.Println("Actual is longer than expected")
				break
			}
			eLine := expectedLines[i]
			if aLine != eLine {
				if i > 0 {
					fmt.Println("         ", actualLines[i-1])
				}
				fmt.Println("  actual:", aLine)
				if i+1 < len(actualLines) {
					fmt.Println("         ", actualLines[i+1])
				}
				fmt.Println()
				if i > 0 {
					fmt.Println("         ", expectedLines[i-1])
				}
				fmt.Println("expected:", eLine)
				if i+1 < len(expectedLines) {
					fmt.Println("         ", expectedLines[i+1])
				}

				for j := range min(len(aLine), len(eLine)) {
					if aLine[j] != eLine[j] {
						fmt.Printf("First diff in line %d at index %d (actual: %c, expected: %c)\n", i, j, aLine[j], eLine[j])
						break
					}
				}
				break
			}
		}
		t.Errorf("Expected: %s\n\nactual: %s", expected, actual)
	}
}

// Asserts that 'actual' is SQL-equal to one of the strings from 'expected'.
func AssertContainsSqlEqual(t *testing.T, expected []string, actual string) {
	for _, el := range expected {
		if IsSqlEqual(el, actual) {
			return
		}
	}

	pp.Println("-- Expected (one of):")
	for i, el := range expected {
		fmt.Printf("%d. %s\n", i+1, SqlPrettyPrint([]byte(el)))
	}
	pp.Println("---- Actual:")
	fmt.Printf("%s\n", SqlPrettyPrint([]byte(actual)))
	t.Errorf("Expected: %v\nActual: %s", expected, actual)
}

// Compares 2 strings for SQL-like equality, which is a bit looser than normal strings ==.
// E.g. "some-prefix A OR B some-suffix" == (SQL-like) "some-prefix B OR A some-suffix"
// It's useful in tests.
// This implementation is not correct in general case, it can return that some more complex
// strings aren't SQL-like equal, when they are, but it's good enough for our simple tests.
// (e.g. it only tries to find permutations of size 2)
func IsSqlEqual(expected, actual string) bool {
	if expected == actual {
		return true
	}
	if len(expected) != len(actual) {
		return false
	}
	splitExpected := strings.Split(expected, " ")
	splitActual := strings.Split(actual, " ")
	if len(splitExpected) != len(splitActual) {
		return false
	}
	for i := 0; i < len(splitExpected); i++ {
		if splitExpected[i] != splitActual[i] {
			// we try to change A OR/AND B into B OR/AND A
			if i+2 >= len(splitExpected) || splitExpected[i+1] != splitActual[i+1] || (splitExpected[i+1] != "OR" && splitExpected[i+1] != "AND") {
				return false
			}

			// we compare a X b with c Y d
			a, b := splitExpected[i], splitExpected[i+2]
			c, d := splitActual[i], splitActual[i+2]
			if a == d && b == c {
				i += 2
				continue
			}
			aTrimmed := strings.TrimRight(strings.TrimLeft(a, "("), ")")
			bTrimmed := strings.TrimRight(strings.TrimLeft(b, "("), ")")
			cTrimmed := strings.TrimRight(strings.TrimLeft(c, "("), ")")
			dTrimmed := strings.TrimRight(strings.TrimLeft(d, "("), ")")
			if aTrimmed != dTrimmed || bTrimmed != cTrimmed {
				return false
			}

			i += 2
		}
	}
	return true
}

func AlmostEmpty(jsonMap JsonMap, acceptableKeys []string) bool {
	for k, v := range jsonMap {
		switch vTyped := v.(type) {
		case JsonMap:
			if !AlmostEmpty(vTyped, acceptableKeys) {
				return false
			}
		default:
			if !slices.Contains(acceptableKeys, k) {
				return false
			}
		}
	}
	return true
}

// Indent returns a string of 'indentLvl' number of tabs
func Indent(indentLvl int) string {
	return strings.Repeat("\t", indentLvl)
}

// Returns a == b, but it's better in 1 way: equal(1, 1.0) == true, equal(1.0, 1) == true
// Useful in comparing JSONs, where we can have 1 and 1.0, and we want them to be equal.
func equal(a, b any) bool {
	if a == b {
		return true
	}

	aFloat, aIsFloat := a.(float64)
	bFloat, bIsFloat := b.(float64)
	if aIsFloat && bIsFloat {
		return aFloat-bFloat < 5e-6 && aFloat-bFloat > -5e-6
	}

	switch aTyped := a.(type) {
	case float64:
		bAsInt, ok := b.(int)
		if ok && aTyped == float64(bAsInt) {
			return true
		}
	case int:
		bAsFloat, ok := b.(float64)
		if ok && float64(aTyped) == bAsFloat {
			return true
		}
	case int64:
		bAsFloat, ok := b.(float64)
		if ok && float64(aTyped) == bAsFloat {
			return true
		}
	case uint64:
		bAsFloat, ok := b.(float64)
		if ok && float64(aTyped) == bAsFloat {
			return true
		}
	}
	return false
}

// ExtractInt64 returns int64 value behind `value`:
// * value,  if it's  (u)int[8|16|32|64]
// * *value, if it's *(u)int[8|16|32|64]
// * -1,     otherwise
// Cases in order from probably most likely to happen to least.
func ExtractInt64(value any) (int64, error) {
	switch valueTyped := value.(type) {
	case int64:
		return valueTyped, nil
	case uint64:
		return int64(valueTyped), nil
	case int:
		return int64(valueTyped), nil
	case *int:
		return int64(*valueTyped), nil
	case *int64:
		return *valueTyped, nil
	case *uint64:
		return int64(*valueTyped), nil
	case int8:
		return int64(valueTyped), nil
	case uint8:
		return int64(valueTyped), nil
	case *int8:
		return int64(*valueTyped), nil
	case *uint8:
		return int64(*valueTyped), nil
	case int16:
		return int64(valueTyped), nil
	case uint16:
		return int64(valueTyped), nil
	case *int16:
		return int64(*valueTyped), nil
	case *uint16:
		return int64(*valueTyped), nil
	case int32:
		return int64(valueTyped), nil
	case uint32:
		return int64(valueTyped), nil
	case *int32:
		return int64(*valueTyped), nil
	case *uint32:
		return int64(*valueTyped), nil
	}
	return -1, fmt.Errorf("ExtractInt64, value of incorrect type. Expected (*)(u)int64, received: %v; type: %T", value, value)
}

// ExtractInt64Maybe returns int64 value behind `value`:
// * value,  if (u)it's int[8|16|32|64]
// * *value, if it's *(u)int[8|16|32|64]
// * -1,     otherwise
// Also, success: true if value was successfully extracted, false otherwise
func ExtractInt64Maybe(value any) (asInt64 int64, success bool) {
	switch valueTyped := value.(type) {
	case int64:
		return valueTyped, true
	case uint64:
		return int64(valueTyped), true
	case int:
		return int64(valueTyped), true
	case *int:
		return int64(*valueTyped), true
	case *int64:
		return *valueTyped, true
	case *uint64:
		return int64(*valueTyped), true
	case int8:
		return int64(valueTyped), true
	case uint8:
		return int64(valueTyped), true
	case *int8:
		return int64(*valueTyped), true
	case *uint8:
		return int64(*valueTyped), true
	case int16:
		return int64(valueTyped), true
	case uint16:
		return int64(valueTyped), true
	case *int16:
		return int64(*valueTyped), true
	case *uint16:
		return int64(*valueTyped), true
	case int32:
		return int64(valueTyped), true
	case uint32:
		return int64(valueTyped), true
	case *int32:
		return int64(*valueTyped), true
	case *uint32:
		return int64(*valueTyped), true
	}
	return -1, false
}

// ExtractFloat64 returns float64 value behind `value`:
// * value,  if it's float64/32
// * *value, if it's *float64/32
// * -1,     otherwise
func ExtractFloat64(value any) (float64, error) {
	switch valueTyped := value.(type) {
	case float64:
		return valueTyped, nil
	case *float64:
		return *valueTyped, nil
	case float32:
		return float64(valueTyped), nil
	case *float32:
		return float64(*valueTyped), nil
	}
	return -1, fmt.Errorf("ExtractFloat64, value of incorrect type. Expected (*)float64, received: %v; type: %T", value, value)
}

// ExtractFloat64Maybe returns float64 value behind `value`:
// * value,  if it's float64/32
// * *value, if it's *float64/32
// * -1,     otherwise
// Also, success: true if value was successfully extracted, false otherwise
func ExtractFloat64Maybe(value any) (asFloat64 float64, success bool) {
	switch valueTyped := value.(type) {
	case float64:
		return valueTyped, true
	case *float64:
		return *valueTyped, true
	case float32:
		return float64(valueTyped), true
	case *float32:
		return float64(*valueTyped), true
	}
	return -1, false
}

// ExtractNumeric64Maybe returns float64 value behind `value`, if it's numeric (some kind of (*)int or (*)float).
func ExtractNumeric64Maybe(value any) (asFloat64 float64, success bool) {
	if asFloat64, success = ExtractFloat64Maybe(value); success {
		return asFloat64, true
	}
	var asInt64 int64
	if asInt64, success = ExtractInt64Maybe(value); success {
		return float64(asInt64), true
	}
	return 0.0, false
}

// ExtractNumeric64 returns float64 value behind `value`, if it's numeric (some kind of (*)int or (*)float).
// Returns 0 if `value` is not numeric.
func ExtractNumeric64(value any) float64 {
	asFloat64, _ := ExtractNumeric64Maybe(value)
	return asFloat64
}

func BoolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func BoolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// SingleQuote is a simple helper function: str -> 'str'
func SingleQuote(value string) string {
	return "'" + value + "'"
}

// IsSingleQuoted checks if a string is single-quoted
func IsSingleQuoted(s string) bool {
	return len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\''
}

// IsQuoted checks if a string is quoted (by ")
func IsQuoted(s string) bool {
	return len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"'
}

// SingleQuoteIfString is a simple helper function: (str -> 'str', other -> other)
func SingleQuoteIfString(value any) any {
	if str, ok := value.(string); ok {
		return SingleQuote(str)
	}
	return value
}

// SurroundWithPercents is a simple helper function: str -> %str%
func SurroundWithPercents(value string) string {
	return "%" + value + "%"
}

// IsSurroundedWithPercents checks if a string has % at the beginning and end
func IsSurroundedWithPercents(value string) bool {
	return len(value) >= 1 && value[0] == '%' && value[len(value)-1] == '%'
}

type sqlMockMismatchSql struct {
	expected string
	actual   string
}

func InitSqlMockWithPrettyPrint(t *testing.T, matchExpectationsInOrder bool) (*sql.DB, sqlmock.Sqlmock) {
	mismatchedSqls := make([]sqlMockMismatchSql, 0)
	lock := sync.Mutex{}
	queryMatcher := sqlmock.QueryMatcherFunc(func(expectedSQL, actualSQL string) error {
		matchErr := sqlmock.QueryMatcherRegexp.Match(expectedSQL, actualSQL)
		if matchErr != nil {
			lock.Lock()
			mismatchedSqls = append(mismatchedSqls, sqlMockMismatchSql{expected: expectedSQL, actual: actualSQL})
			lock.Unlock()
		}
		return matchErr
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(queryMatcher))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			lock.Lock()
			defer lock.Unlock()
			for _, mismatch := range mismatchedSqls {
				pp.Printf("-- %s Expected:\n", t.Name())
				fmt.Printf("%s\n", SqlPrettyPrint([]byte(mismatch.expected)))
				fmt.Printf("RAW: '%s'\n", mismatch.expected)
				pp.Printf("---- %s Actual:\n", t.Name())
				fmt.Printf("%s\n", SqlPrettyPrint([]byte(mismatch.actual)))
				fmt.Printf("Raw: '%s'\n", mismatch.actual)
			}
		}
	})
	mock.MatchExpectationsInOrder(matchExpectationsInOrder)
	return db, mock
}

func InitSqlMockWithPrettySqlAndPrint(t *testing.T, matchExpectationsInOrder bool) (*sql.DB, sqlmock.Sqlmock) {
	mismatchedSqls := make([]sqlMockMismatchSql, 0)
	lock := sync.Mutex{}
	queryMatcher := sqlmock.QueryMatcherFunc(func(expectedSQL, actualSQL string) error {
		expectedPretty := SqlPrettyPrint([]byte(expectedSQL))
		actualSQLWithoutOpt := strings.Split(actualSQL, "-- optimizations")[0]
		actualPretty := SqlPrettyPrint([]byte(actualSQLWithoutOpt))

		var matchError error
		if expectedPretty != actualPretty {
			matchError = fmt.Errorf("sql mismatch")
		}

		if matchError != nil {
			lock.Lock()
			mismatchedSqls = append(mismatchedSqls, sqlMockMismatchSql{expected: expectedPretty, actual: actualPretty})
			lock.Unlock()
		}
		return matchError
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(queryMatcher))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			lock.Lock()
			defer lock.Unlock()
			for _, mismatch := range mismatchedSqls {
				pp.Printf("-- %s Expected pretty:\n", t.Name())
				fmt.Printf("%s\n", mismatch.expected)
				pp.Printf("---- %s Actual pretty:\n", t.Name())
				fmt.Printf("%s\n", mismatch.actual)
			}
		}
	})
	mock.MatchExpectationsInOrder(matchExpectationsInOrder)
	return db, mock
}

func stringifyHelper(v interface{}, isInsideArray bool) string {
	switch v := v.(type) {
	case string:
		if isInsideArray {
			return fmt.Sprintf("\\\"%s\\\"", v)
		} else {
			return fmt.Sprintf("%v", v)
		}
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case []interface{}:
		var parts []string
		for _, elem := range v {
			isInsideArray = true
			parts = append(parts, stringifyHelper(elem, isInsideArray))
			isInsideArray = false
		}
		return fmt.Sprintf("[%s]", strings.Join(parts, ","))
	default:
		return fmt.Sprintf("%v", v)
	}
}

// This functions returns a string from an interface{}.
func Stringify(v interface{}) string {
	const isInsideArray = false
	return stringifyHelper(v, isInsideArray)
}

const timestampFieldName = "@timestamp"

func isLetter(char byte) bool {
	return (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')
}

func isDigit(char byte) bool {
	return char >= '0' && char <= '9'
}

func replaceNonAlphabetic(str string) string {
	chars := []byte(str)
	for i, c := range chars {
		if !isLetter(c) && !isDigit(c) {
			chars[i] = '_'
		}
	}
	return string(chars)
}

func FieldPartToColumnEncoder(field string) string {

	if len(field) == 0 {
		return field
	}
	// Skip timestamp
	if field == timestampFieldName {
		return field
	}
	newField := strings.ToLower(field)
	newField = replaceNonAlphabetic(newField)
	return newField
}

// FieldToColumnEncoder takes input field name
// and converts it using algorithm defined in
// https://github.com/QuesmaOrg/quesma/blob/main/adr/5_nested_fields_representation.md
// all lower case
// all non-alphanumeric are translated to ‘_’ (e.g. “host-name”, “host.name”, “host name” will be “host_name”)
// if starts with digit, then add ‘_’ at beginning
// Save mapping to persitent logic, on-collision do override.
func FieldToColumnEncoder(field string) string {
	newField := FieldPartToColumnEncoder(field)

	if isDigit(newField[0]) {
		newField = "_" + newField
	}

	const maxFieldLength = 256

	if len(newField) > maxFieldLength {
		// TODO maybe we should return error here or truncate the field name
		// for now we just log a warning
		//
		// importing logger causes the circular dependency
		//logger.Warn().Msgf("Field name %s is too long.", newField)

		// TODO So we use log package. We can configure the zerolog logger as a backend for log package.
		log.Println("Field name", newField, "is too long.")
	}

	return newField
}

// ExtractUsernameFromBasicAuthHeader takes the basic auth header and extracts username from it
func ExtractUsernameFromBasicAuthHeader(authHeader string) (string, error) {
	authParts := strings.SplitN(authHeader, " ", 2)
	if len(authParts) != 2 {
		return "", fmt.Errorf("invalid authorization header format")
	}
	if authParts[0] == "Bearer" {
		return "", fmt.Errorf("cannot extract username from Bearer token")
	}
	decodedUserAndPass, err := base64.StdEncoding.DecodeString(authParts[1])
	if err != nil {
		return "", err
	}
	pair := strings.SplitN(string(decodedUserAndPass), ":", 2)
	if len(pair) != 2 {
		return "", fmt.Errorf("invalid decoded authorization format")
	}
	return pair[0], nil
}

var patternCache = make(map[string]*regexp.Regexp)
var patternCacheLock = sync.RWMutex{}

func TableNamePatternRegexp(indexPattern string) *regexp.Regexp {

	patternCacheLock.RLock()

	pattern, ok := patternCache[indexPattern]
	if ok {
		patternCacheLock.RUnlock()
		return pattern
	}

	patternCacheLock.RUnlock()
	patternCacheLock.Lock()
	defer patternCacheLock.Unlock()

	// Clear cache if it's too big
	const maxPatternCacheSize = 1000
	if len(patternCache) > maxPatternCacheSize {
		patternCache = make(map[string]*regexp.Regexp)
	}

	var builder strings.Builder

	for _, char := range indexPattern {
		switch char {
		case '*':
			builder.WriteString(".*")
		case '[', ']', '\\', '^', '$', '.', '|', '?', '+', '(', ')':
			builder.WriteRune('\\')
			builder.WriteRune(char)
		default:
			builder.WriteRune(char)
		}
	}

	result := regexp.MustCompile(fmt.Sprintf("^%s$", builder.String()))
	patternCache[indexPattern] = result
	return result
}

func ReadResponseBody(resp *http.Response) ([]byte, error) {
	var reader io.Reader
	if resp.Header.Get("Content-Encoding") == "gzip" {
		gzipReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer gzipReader.Close()
		reader = gzipReader
	} else {
		reader = resp.Body
	}
	respBody, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewBuffer(respBody))
	return respBody, nil
}

func PrettyTestName(name string, idx int) string {
	return fmt.Sprintf("%s(%d)", name, idx)
}
