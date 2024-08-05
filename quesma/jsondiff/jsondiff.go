// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package jsondiff

import (
	"fmt"
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
	invalidType        = newType("invalid_type", "Types are not equal")
	invalidValue       = newType("invalid_value", "Values are not equal")
	invalidArrayLength = newType("invalid_array_length", "Array lengths are not equal")
	objectDifference   = newType("object_difference", "Objects are different")
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

type JSONDiff struct {
	path     []string
	problems Results

	ignorePaths []*regexp.Regexp
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

func (d *JSONDiff) subtract(a, b []string) []string {
	var c []string

	for _, x := range a {
		found := false
		for _, y := range b {
			if x == y {
				found = true
				break
			}
		}
		if !found {
			c = append(c, x)
		}
	}

	return c
}

func (d *JSONDiff) compareArray(actual []any, expected []any) {

	if len(actual)-len(expected) == 1 || len(actual)-len(expected) == 1 {
		d.addProblem(invalidArrayLength, fmt.Sprintf("%d", len(actual)), fmt.Sprintf("%d", len(expected)))
		// off by one difference, here we can compare the rest of the array
	} else if len(actual) != len(expected) {
		d.addProblem(invalidArrayLength, fmt.Sprintf("%d", len(actual)), fmt.Sprintf("%d", len(expected)))
		return
	}

	// TODO maybe check if the arrays are sorter differently

	for i := range min(len(actual), len(expected)) {
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

func (d *JSONDiff) compare(a any, b any) {
	if d.isIgnoredPath() {
		return
	}

	if a == nil && b == nil {
		return
	}

	if a == nil && b != nil {
		d.addProblem(invalidValue, d.asValue(a), d.asValue(b))
		return
	}

	if a != nil && b == nil {
		d.addProblem(invalidValue, d.asValue(a), d.asValue(b))
		return
	}

	switch aVal := a.(type) {
	case map[string]any:

		switch bVal := b.(type) {
		case map[string]any:

			d.compareObject(aVal, bVal)
		default:
			d.addProblem(invalidType, d.asType(a), d.asType(b))
			return
		}

	case []any:
		switch b.(type) {
		case []any:
			d.compareArray(a.([]any), b.([]any))
		default:
			d.addProblem(invalidType, d.asType(a), d.asType(b))
		}

	default:

		if reflect.TypeOf(a) != reflect.TypeOf(b) {
			d.addProblem(invalidType, d.asValue(a), d.asValue(b))
			return
		}

		// TODO how to compare floats and ints ?

		if a != b {
			d.addProblem(invalidValue, fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
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
