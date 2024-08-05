package jsondiff

import (
	"fmt"
	"quesma/quesma/types"
	"sort"
	"strings"
)

type JSONDiffProblem struct {
	Path string

	Message string

	ContextA string
	ContextB string
	// other
}

type JSONDiffResults []JSONDiffProblem

type JSONDiff struct {
	path     []string
	problems JSONDiffResults

	ignorePaths []string
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

func (d *JSONDiff) addProblem(message string, contextA string, contextB string) {
	problem := JSONDiffProblem{
		Path:     d.pathString(),
		Message:  message,
		ContextA: contextA,
		ContextB: contextB,
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
		d.addProblem("Array lengths are not equal.", fmt.Sprintf("%d", len(actual)), fmt.Sprintf("%d", len(expected)))
		// off by one difference, here we can compare the rest of the array
	} else if len(actual) != len(expected) {
		d.addProblem("Array lengths are not equal", fmt.Sprintf("%d", len(actual)), fmt.Sprintf("%d", len(expected)))
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
		d.addProblem("Missing value", fmt.Sprintf("%s", a), fmt.Sprintf("%s", b))
		return
	}

	switch a.(type) {
	case types.JSON:

		switch b.(type) {
		case types.JSON:
			d.compareObject(a.(types.JSON), b.(types.JSON))
		default:
			d.addProblem("Types are not equal", fmt.Sprintf("%T", a), fmt.Sprintf("%T", b))
			return
		}
	case []any:

		switch b.(type) {
		case []any:
			d.compareArray(a.([]any), b.([]any))
		default:
			d.addProblem("Types are not equal", fmt.Sprintf("%T", a), fmt.Sprintf("%T", b))
		}

	default:
		if a != b {
			d.addProblem("Values are not equal", fmt.Sprintf("%s", a), fmt.Sprintf("%s", b))
		}
	}
}

func (d *JSONDiff) compareObject(expected types.JSON, actual types.JSON) {

	keysA := d.keySet(expected)
	keysB := d.keySet(actual)

	intersect := d.intersect(keysA, keysB)

	if len(intersect) == 0 {
		d.addProblem("Object are different. No common properties found.", strings.Join(keysA, ", "), strings.Join(keysB, ", "))
		return
	}

	for _, k := range keysA {
		d.pushPath(k)
		d.compare(expected[k], actual[k])
		d.popPath()
	}

}

func (d *JSONDiff) CompareJSON(expected types.JSON, actual types.JSON) (JSONDiffResults, error) {

	d.compare(expected, actual)
	return d.problems, nil
}
