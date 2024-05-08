package util

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mitmproxy/quesma/logger"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
)

type JsonMap = map[string]interface{}

func Truncate(body string) string {
	if len(body) < 70 {
		return body
	}
	return body[:70]
}

func IsValidJson(jsonStr string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(jsonStr), &js) == nil
}

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
// * compareBaseTypes - if true, we compare base type values as well (e.g. if mActual["key1"]["key2"] == 1,
// and mExpected["key1"]["key2"] == 2, we say that they are different)
// * compareFullArrays - if true, we compare entire arrays, if false just first element ([0])
// * mActual - uses JsonMap fully: values are []JsonMap, or JsonMap, or base types
// * mExpected - value can also be []any, because it's generated from Golang's json.Unmarshal
func MapDifference(mActual, mExpected JsonMap, compareBaseTypes, compareFullArrays bool) (JsonMap, JsonMap) {
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
				addToResult(mActualCur[name], name, keysNested, resultDiffThis)
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
				if compareBaseTypes && !equal(vActual, vExpected) {
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
	actualMinusExpected, expectedMinusActual := MapDifference(mActual, mExpected, false, false)
	return actualMinusExpected, expectedMinusActual, nil
}

// If there's type conflict, e.g. one map has {"a": map}, and second has {"a": array}, we log an error.
// Tried https://stackoverflow.com/a/71545414 and https://stackoverflow.com/a/71652767
// but none of them works for nested maps, so needed to write our own.
// * mActual - uses JsonMap fully: values are []JsonMap, or JsonMap, or base types
// * mExpected - value can also be []any, because it's generated from Golang's json.Unmarshal
func MergeMaps(ctx context.Context, mActual, mExpected JsonMap) JsonMap {
	var mergeMapsRec func(m1, m2 JsonMap) JsonMap
	// merges 'i1' and 'i2' in 3 cases: both are JsonMap, both are []JsonMap, or both are some base type
	mergeAny := func(i1, i2 any) any {
		switch i1Typed := i1.(type) {
		case JsonMap:
			i2Typed, ok := i2.(JsonMap)
			if !ok {
				logger.ErrorWithCtx(ctx).Msgf("mergeAny: i1 is map, i2 is not. i1: %v, i2: %v", i1, i2)
				return i1
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
					logger.ErrorWithCtx(ctx).Msgf("mergeAny: i1 is []JsonMap, i2 is not an array. i1: %v, i2: %v", i1Typed, i2)
				}
			}

			// lengths should be always equal in our usage of this function, maybe that'll change
			if len(i1Typed) != len(i2Typed) {
				logger.ErrorWithCtx(ctx).Msgf("mergeAny: i1 and i2 are slices, but have different lengths. i1: %v, i2: %v", i1, i2)
				return []JsonMap{}
			}
			mergedArray := make([]JsonMap, len(i1Typed))
			for i := range i1Typed {
				mergedArray[i] = mergeMapsRec(i1Typed[i], i2Typed[i].(JsonMap))
			}
			return mergedArray
		default:
			return i1
		}
	}

	mergeMapsRec = func(m1, m2 JsonMap) JsonMap {
		mergedMap := make(JsonMap)
		for k, v1 := range m1 {
			v2, ok := m2[k]
			if ok {
				mergedMap[k] = mergeAny(v1, v2)
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
		return mergedMap
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
		t.Errorf("Expected: %s, got: %s", expected, actual)
	}
}

// Asserts that 'actual' is SQL-equal to one of the strings from 'expected'.
func AssertContainsSqlEqual(t *testing.T, expected []string, actual string) {
	for _, el := range expected {
		if IsSqlEqual(el, actual) {
			return
		}
	}
	t.Errorf("Expected: %v, got: %s", expected, actual)
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

// Returns a string of 'indentLvl' number of tabs
func Indent(indentLvl int) string {
	return strings.Repeat("\t", indentLvl)
}

// Returns Kind of type from passed string
// Example : KindFromString("Int")
func KindFromString(typeName string) (reflect.Kind, error) {
	switch typeName {
	case "Int64", "Int":
		return reflect.Int, nil
	case "Float64":
		return reflect.Float64, nil
	case "String":
		return reflect.String, nil
	default:
		return reflect.Invalid, fmt.Errorf("unsupported type: %s", typeName)
	}
}

// Checks whether passed value is a float type
// with zeros after decimal point
// Example: 1.00 will return true
// Example: 1.54 will return false
func IsInt(value interface{}) bool {
	// Get the type of the value
	valueType := reflect.TypeOf(value)

	// Check if the type is float64
	if valueType.Kind() == reflect.Float64 {
		// Convert the float value to string
		stringValue := fmt.Sprintf("%f", value)
		// Split the string by decimal point
		parts := strings.Split(stringValue, ".")
		if len(parts) == 2 && len(parts[1]) > 0 {
			// Check if the decimal part contains only zeros
			for _, digit := range parts[1] {
				if digit != '0' {
					return false
				}
			}
		}
		return true
	}
	return valueType.Kind() == reflect.Int || valueType.Kind() == reflect.Int64 || valueType.Kind() == reflect.Int32
}

// Function returns a type - kind for specific value
// passed as a parameter
func ValueKind(value interface{}) reflect.Kind {
	return reflect.TypeOf(value).Kind()
}

// Returns a == b, but it's better in 1 way: equal(1, 1.0) == true, equal(1.0, 1) == true
// Useful in comparing JSONs, where we can have 1 and 1.0, and we want them to be equal.
func equal(a, b any) bool {
	if a == b {
		return true
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
func ExtractInt64(value any) int64 {
	switch valueTyped := value.(type) {
	case int64:
		return valueTyped
	case uint64:
		return int64(valueTyped)
	case *int64:
		return *valueTyped
	case *uint64:
		return int64(*valueTyped)
	case int8:
		return int64(valueTyped)
	case uint8:
		return int64(valueTyped)
	case *int8:
		return int64(*valueTyped)
	case *uint8:
		return int64(*valueTyped)
	case int16:
		return int64(valueTyped)
	case uint16:
		return int64(valueTyped)
	case *int16:
		return int64(*valueTyped)
	case *uint16:
		return int64(*valueTyped)
	case int32:
		return int64(valueTyped)
	case uint32:
		return int64(valueTyped)
	case *int32:
		return int64(*valueTyped)
	case *uint32:
		return int64(*valueTyped)
	}
	logger.Error().Msgf("ExtractInt64, value of incorrect type. Expected (*)(u)int64, received: %v; type: %T", value, value)
	return -1
}

// ExtractFloat64 returns float64 value behind `value`:
// * value,  if it's float64/32
// * *value, if it's *float64/32
// * -1,     otherwise
func ExtractFloat64(value any) float64 {
	switch valueTyped := value.(type) {
	case float64:
		return valueTyped
	case *float64:
		return *valueTyped
	case float32:
		return float64(valueTyped)
	case *float32:
		return float64(*valueTyped)
	}
	logger.Error().Msgf("ExtractFloat64, value of incorrect type. Expected (*)float64, received: %v; type: %T", value, value)
	return -1
}
