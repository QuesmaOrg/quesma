package jsondiff

import (
	"fmt"
	"quesma/quesma/types"
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
	missingValue       = newType("missing_value", "Missing value")
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
	// other
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

	ignorePaths []string
}

func NewJSONDiff(ignorePaths ...string) *JSONDiff {

	return &JSONDiff{
		ignorePaths: ignorePaths,
	}
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
	for _, k := range d.ignorePaths {
		if k == p {
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

func (d *JSONDiff) compareArray(actual []any, expected []any) {

	if len(actual)-len(expected) == 1 || len(actual)-len(expected) == 1 {
		d.addProblem(invalidArrayLength, fmt.Sprintf("%d", len(actual)), fmt.Sprintf("%d", len(expected)))
		// off by one difference, here we can compare the rest of the array
	} else if len(actual) != len(expected) {
		d.addProblem(invalidArrayLength, fmt.Sprintf("%d", len(actual)), fmt.Sprintf("%d", len(expected)))
		return
	}

	for i := range min(len(actual), len(expected)) {
		d.pushPath(fmt.Sprintf("[%d]", i))
		d.compare(actual[i], expected[i])
		d.popPath()
	}
}

func (d *JSONDiff) compare(a any, b any) {
	if d.isIgnoredPath() {
		return
	}

	if a == nil && b == nil {
		return
	}

	if b == nil {
		d.addProblem(missingValue, fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
		return
	}

	switch aVal := a.(type) {
	case map[string]any:

		switch bVal := b.(type) {
		case map[string]any:

			d.compareObject(aVal, bVal)
		default:
			d.addProblem(invalidType, fmt.Sprintf("%T", a), fmt.Sprintf("%T", b))
			return
		}

	case []any:
		switch b.(type) {
		case []any:
			d.compareArray(a.([]any), b.([]any))
		default:
			d.addProblem(invalidType, fmt.Sprintf("%T", a), fmt.Sprintf("%T", b))
		}

	default:
		if a != b {
			d.addProblem(invalidValue, fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
		}
	}
}

func (d *JSONDiff) compareObject(expected map[string]any, actual map[string]any) {

	keysA := d.keySet(expected)
	keysB := d.keySet(actual)

	intersect := d.intersect(keysA, keysB)

	if len(intersect) == 0 {
		d.addProblem(objectDifference, strings.Join(keysA, ", "), strings.Join(keysB, ", "))
		return
	}

	for _, k := range keysA {
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
