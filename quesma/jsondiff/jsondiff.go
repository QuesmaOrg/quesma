// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package jsondiff

import (
	"fmt"
	"math"
	"quesma/quesma/types"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

type problemType struct {
	code    string
	message string
}

func newType(code, message string) problemType {
	return problemType{code, message}
}

var (
	invalidType         = newType("invalid_type", "Types are not equal")
	invalidValue        = newType("invalid_value", "Values are not equal")
	invalidArrayLength  = newType("invalid_array_length", "Array lengths are not equal")
	objectDifference    = newType("object_difference", "Objects are different")
	arrayDifference     = newType("array_difference", "Array elements are different")
	arraySortDifference = newType("array_sort_difference", "Array elements are sorted differently")
)

type Problem struct {
	Path string

	Type    string
	Message string

	Expected string
	Actual   string

	// TODO: add more context,
}

func (p Problem) String() string {
	return fmt.Sprintf("%s: %s, expected: %s, actual: %s", p.Path, p.Message, p.Expected, p.Actual)
}

type Results []Problem

func (r Results) String() string {
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
	path     []string
	problems Results

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
	// regexp match ?
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

func (d *JSONDiff) addProblem(problemType problemType, expected string, actual string) {
	problem := Problem{
		Path:    d.pathString(),
		Message: problemType.message,
		Type:    problemType.code,

		Expected: expected,
		Actual:   actual,
	}
	d.problems = append(d.problems, problem)
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

	for _, x := range a {
		for _, y := range b {
			if x == y {
				c = append(c, x)
				break
			}
		}
	}

	return c
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

func (d *JSONDiff) compareArrayByElementKeys(expected []any, actual []any) bool {

	keyExtractor := d.findKeyExtractor()

	if keyExtractor != nil {
		return false
	}

	var expectedKeys []string
	for _, a := range expected {
		key, err := keyExtractor(a)
		if err != nil {
			return false
		}
		expectedKeys = append(expectedKeys, key)
	}

	var actualKeys []string
	for _, a := range actual {
		key, err := keyExtractor(a)
		if err != nil {
			return false
		}
		actualKeys = append(actualKeys, key)
	}

	commonKeys := d.intersect(expectedKeys, actualKeys)

	if len(commonKeys) == 0 {
		d.addProblem(arrayDifference,
			fmt.Sprintf("Keys: %s", strings.Join(expectedKeys, ", ")),
			fmt.Sprintf("Keys: %s", strings.Join(actualKeys, ", ")))
		return true
	}

	if d.compareStringArrays(expectedKeys, actualKeys) == false && d.compareStringsArrayOmitOrder(expectedKeys, actualKeys) {

		d.addProblem(arraySortDifference,
			fmt.Sprintf("Keys: %s", strings.Join(expectedKeys, ", ")),
			fmt.Sprintf("Keys: %s", strings.Join(actualKeys, ", ")))
		return true
	}

	return false
}

func (d *JSONDiff) compareArray(expected []any, actual []any) {

	if len(actual) != len(expected) {
		d.addProblem(invalidArrayLength, fmt.Sprintf("%d", len(actual)), fmt.Sprintf("%d", len(expected)))
		return
	}

	if len(actual) == 0 {
		return
	}

	if d.compareArrayByElementKeys(expected, actual) {
		return
	}

	for i := range len(actual) {
		d.pushPath(fmt.Sprintf("[%d]", i))
		d.compare(actual[i], expected[i])
		d.popPath()
	}
}

func (d *JSONDiff) asValue(a any) string {
	return fmt.Sprintf("%v", a)
}

func (d *JSONDiff) asType(a any) string {
	return fmt.Sprintf("%T", a)
}

func (d *JSONDiff) compare(expected any, actual any) {
	if d.isIgnoredPath() {
		return
	}

	if expected == nil && actual == nil {
		return
	}

	if expected == nil && actual != nil {
		d.addProblem(invalidValue, d.asValue(expected), d.asValue(actual))
		return
	}

	if expected != nil && actual == nil {
		d.addProblem(invalidValue, d.asValue(expected), d.asValue(actual))
		return
	}

	switch aVal := expected.(type) {
	case map[string]any:

		switch bVal := actual.(type) {
		case map[string]any:

			d.compareObject(aVal, bVal)
		default:
			d.addProblem(invalidType, d.asType(expected), d.asType(actual))
			return
		}

	case []any:
		switch actual.(type) {
		case []any:
			d.compareArray(expected.([]any), actual.([]any))
		default:
			d.addProblem(invalidType, d.asType(expected), d.asType(actual))
		}

	case float64:
		switch actual.(type) {
		case float64:

			// float operations are noisy, we need to compare them with desired precision

			epsilon := 1e-9
			relativeTolerance := 1e-9
			aFloat := expected.(float64)
			bFloat := actual.(float64)

			absDiff := math.Abs(aFloat - bFloat)
			if absDiff > epsilon {
				// Relative tolerance check
				relativeDiff := absDiff / math.Max(math.Abs(aFloat), math.Abs(bFloat))

				if relativeDiff > relativeTolerance {
					d.addProblem(invalidValue, d.asValue(expected), d.asValue(actual))
				}
			}

		default:
			d.addProblem(invalidType, d.asType(expected), d.asType(actual))
		}

	default:

		if reflect.TypeOf(expected) != reflect.TypeOf(actual) {
			d.addProblem(invalidType, d.asValue(expected), d.asValue(actual))
			return
		}

		if expected != actual {
			d.addProblem(invalidValue, fmt.Sprintf("%v", expected), fmt.Sprintf("%v", actual))
		}
	}
}

func (d *JSONDiff) compareObject(expected map[string]any, actual map[string]any) {

	expectedKeys := d.keySet(expected)
	actualKeys := d.keySet(actual)

	commonKeys := d.intersect(expectedKeys, actualKeys)

	if len(commonKeys) == 0 {
		d.addProblem(objectDifference, strings.Join(expectedKeys, ", "), strings.Join(actualKeys, ", "))
		return
	}

	// TODO what keys should we compare?

	for _, k := range expectedKeys {
		d.pushPath(k)
		d.compare(expected[k], actual[k])
		d.popPath()
	}
}

func (d *JSONDiff) Diff(expected types.JSON, actual types.JSON) (Results, error) {

	// There is a problem with our JSON type. The root is a types.JSON, but objects inside are map[string]any.
	// We need to convert the types.JSON to map[string]any
	expectedMap := map[string]any(expected)
	actualMap := map[string]any(actual)

	d.compare(expectedMap, actualMap)
	return d.problems, nil
}
