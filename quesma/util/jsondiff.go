// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"fmt"
	"math"
	"time"

	"github.com/QuesmaOrg/quesma/quesma/quesma/types"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

type mismatchType struct {
	code    string
	message string
}

func newType(code, message string) mismatchType {
	return mismatchType{code, message}
}

var (
	invalidType                 = newType("invalid_type", "Types are not equal")
	invalidValue                = newType("invalid_value", "Values are not equal")
	invalidNumberValue          = newType("invalid_number_value", "Numbers are not equal")
	invalidDateValue            = newType("invalid_date_value", "Dates are not equal")
	invalidArrayLength          = newType("invalid_array_length", "Array lengths are not equal")
	invalidArrayLengthOffByOne  = newType("invalid_array_length_off_by_one", "Array lengths are off by one.")
	objectDifference            = newType("object_difference", "Objects are different")
	arrayKeysDifference         = newType("array_keys_difference", "Array keys are different")
	arrayKeysDifferenceSlightly = newType("array_keys_difference_slightly", "Array keys are slightly different")
	arrayKeysSortDifference     = newType("array_keys_sort_difference", "Array keys are sorted differently")
)

type JSONMismatch struct {
	Path string

	Type    string
	Message string

	Expected string
	Actual   string

	// TODO: add more context,
}

func (p JSONMismatch) String() string {
	return fmt.Sprintf("%s: %s, expected: %s, actual: %s", p.Path, p.Message, p.Expected, p.Actual)
}

type Mismatches []JSONMismatch

func (r Mismatches) String() string {
	var s string
	for _, p := range r {
		s += p.String() + "\n"
	}
	return s
}

type pathKeyExtractor struct {
	rx           *regexp.Regexp
	keyExtractor func(any) (string, error)
}

type JSONDiff struct {
	path       []string
	mismatches Mismatches

	ignorePaths       []*regexp.Regexp
	pathKeyExtractors []pathKeyExtractor
}

func NewJSONDiff(ignorePaths ...string) (*JSONDiff, error) {

	var ignorePathRegex []*regexp.Regexp

	for _, p := range ignorePaths {
		rx, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("regexp '%s' compilation failed: %v", p, err)
		}
		ignorePathRegex = append(ignorePathRegex, rx)
	}

	return &JSONDiff{
		ignorePaths: ignorePathRegex,
	}, nil
}

func (d *JSONDiff) pushPath(path string) {
	d.path = append(d.path, path)
}

func (d *JSONDiff) popPath() {
	d.path = d.path[:len(d.path)-1]
}

func (d *JSONDiff) pathString() string {
	return strings.Join(d.path, ".")
}

func (d *JSONDiff) isIgnoredPath() bool {
	p := d.pathString()
	// regexp match ?
	for _, rx := range d.ignorePaths {
		if rx.MatchString(p) {
			return true
		}
	}
	return false
}

func (d *JSONDiff) findKeyExtractor() func(any) (string, error) {
	p := d.pathString()

	for _, x := range d.pathKeyExtractors {
		if x.rx.MatchString(p) {
			return x.keyExtractor
		}
	}
	return nil
}

func (d *JSONDiff) AddKeyExtractor(str string, keyExtractor func(any) (string, error)) error {
	rx, err := regexp.Compile(str)
	if err != nil {
		return fmt.Errorf("regexp '%s' compilation failed: %v", str, err)
	}
	d.pathKeyExtractors = append(d.pathKeyExtractors, pathKeyExtractor{rx, keyExtractor})
	return nil
}

func (d *JSONDiff) addMismatch(mismatchType mismatchType, expected string, actual string) {
	m := JSONMismatch{
		Path:    d.pathString(),
		Message: mismatchType.message,
		Type:    mismatchType.code,

		Expected: expected,
		Actual:   actual,
	}
	d.mismatches = append(d.mismatches, m)
}

func (d *JSONDiff) keySet(a types.JSON) []string {
	var keys []string

	for k := range a {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func (d *JSONDiff) intersect(a, b []string) []string {
	var c []string

	elementCounts := make(map[string]int)

	// Count all elements from slice 'a'
	for _, x := range a {
		elementCounts[x]++
	}

	// Check each element in slice 'b' against the map
	for _, y := range b {
		if count, exists := elementCounts[y]; exists && count > 0 {
			c = append(c, y)
			elementCounts[y] = 0 // Ensure each element is added only once to the result
		}
	}

	return c
}

func (d *JSONDiff) sum(a, b []string) []string {
	var result []string

	elementCounts := make(map[string]bool)

	// Count all elements from slice 'a'
	for _, x := range a {
		elementCounts[x] = true
		result = append(result, x)
	}

	// Check each element in slice 'b' against the map
	for _, y := range b {
		if _, exists := elementCounts[y]; !exists {
			result = append(result, y)
		}
	}

	return result
}

func (d *JSONDiff) compareStringArrays(a, b []string) bool {

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func (d *JSONDiff) formatListOfKeys(keys []string) string {

	var quotedKeys []string
	for _, k := range keys {
		quotedKeys = append(quotedKeys, fmt.Sprintf("'%s'", k))
	}

	return strings.Join(quotedKeys, ", ")
}

func (d *JSONDiff) compareStringsArrayOmitOrder(a, b []string) bool {

	if len(a) != len(b) {
		return false
	}

	aCopy := make([]string, len(a))
	bCopy := make([]string, len(b))
	aCopy = append(aCopy, a...)
	bCopy = append(bCopy, b...)

	sort.Strings(aCopy)
	sort.Strings(bCopy)

	return d.compareStringArrays(aCopy, bCopy)
}

// converts list of elements to list of keys
func (d *JSONDiff) extractKeysFromArray(input []any, keyExtractor func(any) (string, error)) (result []string, ok bool) {
	var keys []string
	var keysMap = make(map[string]bool)
	for _, element := range input {
		key, err := keyExtractor(element)
		if err != nil {
			return result, false
		}
		keys = append(keys, key)
		keysMap[key] = true
	}
	if len(keysMap) < len(keys) {
		// some keys are duplicated
		// we cannot compare the arrays by keys
		return result, false
	}
	return keys, true
}

func (d *JSONDiff) compareArrayByElementKeys(expected []any, actual []any) bool {

	keyExtractor := d.findKeyExtractor()

	if keyExtractor == nil {
		return false
	}

	expectedKeys, ok := d.extractKeysFromArray(expected, keyExtractor)
	if !ok {
		return false
	}

	actualKeys, ok := d.extractKeysFromArray(actual, keyExtractor)
	if !ok {
		return false
	}

	commonKeys := d.intersect(expectedKeys, actualKeys)

	// some tests if the key sets are different
	if len(commonKeys) != len(expectedKeys) {

		// if where is no common keys, we deal with a totally different arrays
		if len(commonKeys) == 0 {
			d.addMismatch(arrayKeysDifference,
				fmt.Sprintf("Keys: %s", d.formatListOfKeys(expectedKeys)),
				fmt.Sprintf("Keys: %s", d.formatListOfKeys(actualKeys)))
			return true
		}

		// this is heuristic,
		// for more than 5 keys eyeballing can be difficult
		// we like to know if the arrays are similar
		if len(expectedKeys) > 5 && len(commonKeys) > len(expectedKeys)-2 {
			d.addMismatch(arrayKeysDifferenceSlightly,
				fmt.Sprintf("Keys: %s", d.formatListOfKeys(expectedKeys)),
				fmt.Sprintf("Keys: %s", d.formatListOfKeys(actualKeys)))
			return true
		}
	}

	// here we can compare if keys are sorted differently
	if !d.compareStringArrays(expectedKeys, actualKeys) && d.compareStringsArrayOmitOrder(expectedKeys, actualKeys) {

		d.addMismatch(arrayKeysSortDifference,
			fmt.Sprintf("Keys: %s", d.formatListOfKeys(expectedKeys)),
			fmt.Sprintf("Keys: %s", d.formatListOfKeys(actualKeys)))
		return true
	}

	return false
}

func (d *JSONDiff) compareArray(expected []any, actual []any) {

	lenDiff := len(actual) - len(expected)
	if lenDiff < 0 {
		lenDiff = -lenDiff
	}

	if lenDiff > 1 {
		d.addMismatch(invalidArrayLength, fmt.Sprintf("%d", len(expected)), fmt.Sprintf("%d", len(actual)))
		return
	} else if lenDiff == 1 {
		d.addMismatch(invalidArrayLengthOffByOne, fmt.Sprintf("%d", len(expected)), fmt.Sprintf("%d", len(actual)))
		return
	}

	if len(actual) == 0 {
		return
	}

	// before comparing the elements of the array, we can try to compare the keys of the elements
	// if the keys are different, we can skip the comparison of the elements

	if d.compareArrayByElementKeys(expected, actual) {
		return
	}

	for i := range len(actual) {
		d.pushPath(fmt.Sprintf("[%d]", i))
		d.compare(expected[i], actual[i])
		d.popPath()
	}
}

func (d *JSONDiff) asValue(a any) string {
	return fmt.Sprintf("%v", a)
}

func (d *JSONDiff) asType(a any) string {
	return fmt.Sprintf("%T", a)
}

var dateRx = regexp.MustCompile(`\d{4}-\d{2}-\d{2}.\d{2}:\d{2}:`)

func (d *JSONDiff) uniformTimeFormat(date string) string {
	returnFormat := time.RFC3339Nano

	inputFormats := []string{
		"2006-01-02T15:04:05.000-07:00",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05",
	}

	var parsedDate time.Time
	var err error
	for _, format := range inputFormats {
		parsedDate, err = time.Parse(format, date)
		if err == nil {
			return parsedDate.UTC().Format(returnFormat)
		}
	}
	return date
}

func (d *JSONDiff) compare(expected any, actual any) {

	if d.isIgnoredPath() {
		return
	}

	if expected == nil && actual == nil {
		return
	}

	if expected == nil && actual != nil {
		d.addMismatch(invalidValue, d.asValue(expected), d.asValue(actual))
		return
	}

	if expected != nil && actual == nil {
		d.addMismatch(invalidValue, d.asValue(expected), d.asValue(actual))
		return
	}

	switch expectedVal := expected.(type) {
	case map[string]any:

		switch bVal := actual.(type) {
		case map[string]any:
			d.compareObject(expectedVal, bVal)
		default:
			d.addMismatch(invalidType, d.asType(expected), d.asType(actual))
			return
		}

	case []any:
		switch actual.(type) {
		case []any:
			d.compareArray(expected.([]any), actual.([]any))
		default:
			d.addMismatch(invalidType, d.asType(expected), d.asType(actual))
		}

	case float64:
		switch actual.(type) {
		case float64:

			// float operations are noisy, we need to compare them with desired precision
			// this is lousy, but it works for now
			epsilon := 1e-3
			relativeTolerance := 1e-3
			aFloat := expected.(float64)
			bFloat := actual.(float64)

			absDiff := math.Abs(aFloat - bFloat)
			if absDiff > epsilon {
				// Relative tolerance check
				relativeDiff := absDiff / math.Max(math.Abs(aFloat), math.Abs(bFloat))

				if relativeDiff > relativeTolerance {
					d.addMismatch(invalidNumberValue, d.asValue(expected), d.asValue(actual))
				}
			}

		default:
			d.addMismatch(invalidType, d.asType(expected), d.asType(actual))
		}

	case string:

		switch actualString := actual.(type) {
		case string:

			if dateRx.MatchString(expectedVal) {

				aDate := d.uniformTimeFormat(expectedVal)
				bDate := d.uniformTimeFormat(actualString)

				if aDate != bDate {
					d.addMismatch(invalidDateValue, d.asValue(expected), d.asValue(actual))
				}

				return
			}

		default:
			d.addMismatch(invalidType, d.asType(expected), d.asType(actual))
		}

	default:

		if reflect.TypeOf(expected) != reflect.TypeOf(actual) {
			d.addMismatch(invalidType, d.asValue(expected), d.asValue(actual))
			return
		}

		if expected != actual {
			d.addMismatch(invalidValue, fmt.Sprintf("%v", expected), fmt.Sprintf("%v", actual))
		}
	}
}

func (d *JSONDiff) compareObject(expected map[string]any, actual map[string]any) {

	expectedKeys := d.keySet(expected)
	actualKeys := d.keySet(actual)

	commonKeys := d.intersect(expectedKeys, actualKeys)

	if len(commonKeys) == 0 {
		d.addMismatch(objectDifference, strings.Join(expectedKeys, ", "), strings.Join(actualKeys, ", "))
		return
	}

	allKeys := d.sum(expectedKeys, actualKeys)
	sort.Strings(allKeys)

	for _, k := range allKeys {
		d.pushPath(k)
		d.compare(expected[k], actual[k])
		d.popPath()
	}
}

func (d *JSONDiff) Diff(expected types.JSON, actual types.JSON) (Mismatches, error) {

	// There is a problem with our JSON type. The root is a types.JSON, but objects inside are map[string]any.
	// We need to convert the types.JSON to map[string]any
	expectedMap := map[string]any(expected)
	actualMap := map[string]any(actual)

	d.compare(expectedMap, actualMap)

	// TODO do we need any limit on the number of mismatches?
	// TODO do we need to sort the mismatches?
	// TODO do we need to compact the mismatches?

	return d.mismatches, nil
}
