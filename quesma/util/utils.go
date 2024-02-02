package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

type JsonMap map[string]interface{}

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
		return fmt.Sprintf("Error unmarshalling JSON: %v", err)
	}

	for k, nested := range jsonData {
		if shorten {
			jsonData[k] = Shorten(nested)
		}
	}
	v, err := json.Marshal(jsonData)
	if err != nil {
		return fmt.Sprintf("Error marshalling JSON: %v", err)
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
				for i := 0; i < min(lenActual, lenExpected); i++ {
					keysNested = append(keysNested, name+"["+strconv.Itoa(i)+"]")
					// assumption: elements of arrays are maps. From observations, it's always true.
					// better to assume that until it breaks at least once. Fixing would require a lot of new code.
					descendRec(vActualTyped[i].(JsonMap), (vExpectedArr)[i].(JsonMap), resultDiffThis, resultDiffOther, keysNested)
					keysNested = keysNested[:len(keysNested)-1]
				}
				for i := min(lenActual, lenExpected); i < lenActual; i++ {
					addToResult(vActualTyped[i], name+"["+strconv.Itoa(i)+"]", keysNested, resultDiffThis)
				}
				for i := min(lenActual, lenExpected); i < lenExpected; i++ {
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
		return nil, nil, err
	}
	mExpected, err := JsonToMap(jsonExpected)
	if err != nil {
		return nil, nil, err
	}
	actualMinusExpected, expectedMinusActual := MapDifference(mActual, mExpected)
	return actualMinusExpected, expectedMinusActual, nil
}
