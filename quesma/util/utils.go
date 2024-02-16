package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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

// returns pair of maps with fields that are present in one of input maps and not in the other
// specifically (mActual - mExpected, mExpected - mActual)
func MapDifference(mActual, mExpected JsonMap) (JsonMap, JsonMap) {
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
			case []interface{}:
				vExpectedArr, ok := vExpected.([]interface{})
				if !ok {
					// Just a safe check. If they are different types, we add to both results. Can be changed, but they shouldn't be different anyway.
					addToResult(vActualTyped, name, keysNested, resultDiffThis)
					addToResult(vExpected, name, keysNested, resultDiffOther)
				}
				lenActual, lenExpected := len(vActualTyped), len(vExpectedArr)
				for i := 0; i < min(1, lenActual, lenExpected); i++ {
					// Code below doesn't cover 100% of cases. But covers all we see, so I don't want to
					// waste time improving it until there's need for it.
					// Assumption: elements of arrays are maps or base types. From observations, it's always true.
					// better to assume that until it breaks at least once. Fixing would require a lot of new code.
					actualArrElementAsMap, ok1 := vActualTyped[i].(JsonMap)
					expectedArrElementAsMap, ok2 := vExpectedArr[i].(JsonMap)
					if ok1 && ok2 {
						keysNested = append(keysNested, name+"["+strconv.Itoa(i)+"]")
						descendRec(actualArrElementAsMap, expectedArrElementAsMap, resultDiffThis, resultDiffOther, keysNested)
						keysNested = keysNested[:len(keysNested)-1]
					} else if ok1 {
						addToResult(actualArrElementAsMap, name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffThis)
					} else if ok2 {
						addToResult(expectedArrElementAsMap, name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffOther)
					}
				}
				for i := min(lenActual, lenExpected); i < min(1, lenActual); i++ {
					addToResult(vActualTyped[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffThis)
				}
				for i := min(lenActual, lenExpected); i < min(1, lenExpected); i++ {
					addToResult(vExpectedArr[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffOther)
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
	actualMinusExpected, expectedMinusActual := MapDifference(mActual, mExpected)
	return actualMinusExpected, expectedMinusActual, nil
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
	split1 := strings.Split(expected, " ")
	split2 := strings.Split(actual, " ")
	if len(split1) != len(split2) {
		return false
	}
	for i := 0; i < len(split1); i++ {
		if split1[i] != split2[i] {
			// we try to change A OR/AND B into B OR/AND A
			if i+2 >= len(split1) {
				return false
			}

			// we compare a X b with c Y d
			a, b := split1[i], split1[i+2]
			c, d := split2[i], split2[i+2]
			if a == d && b == c {
				i += 2
				continue
			}
			if split1[i+1] != split2[i+1] || (split1[i+1] != "OR" && split1[i+1] != "AND") || len(a) != len(d) || len(b) != len(c) {
				return false
			}
			aTrimmed, cTrimmed := strings.TrimLeft(a, "("), strings.TrimLeft(c, "(")
			bTrimmed, dTrimmed := strings.TrimRight(b, ")"), strings.TrimRight(d, ")")
			if aTrimmed != dTrimmed || bTrimmed != cTrimmed {
				return false
			}

			i += 2
		}
	}
	return true
}

// Returns a string of 'indentLvl' number of tabs
func Indent(indentLvl int) string {
	return strings.Repeat("\t", indentLvl)
}
