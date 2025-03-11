// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/common_table"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/schema"
	"github.com/QuesmaOrg/quesma/platform/util"
	"reflect"
	"slices"
	"strings"
	"time"
)

type (
	QueryResultCol struct {
		ColName string // quoted, e.g. `"message"`
		Value   interface{}
		ColType schema.QuesmaType
	}
	QueryResultRow struct {
		Index string
		Cols  []QueryResultCol
	}
)

func NewQueryResultCol(colName string, value interface{}) QueryResultCol {
	return QueryResultCol{ColName: colName, Value: value}
}

// String returns the string representation of the column in format `"<colName>": <value>`, properly quoted.
func (c *QueryResultCol) String(ctx context.Context) string {
	valueExtracted := c.ExtractValue()
	if valueExtracted == nil {
		return ""
	}
	switch valueExtracted.(type) {
	case string:
		processed, err := json.Marshal(valueExtracted)
		if err != nil {
			logger.ErrorWithCtx(ctx).Err(err).Msgf("failed to marshal value %v", valueExtracted)
		}
		return fmt.Sprintf(`"%s": %s`, c.ColName, string(processed))
	case time.Time:
		return fmt.Sprintf(`"%s": "%v"`, c.ColName, valueExtracted)
	case int, int64, float64, uint64, bool:
		return fmt.Sprintf(`"%s": %v`, c.ColName, valueExtracted)
	default:
		// Probably good to only use marshaller when necessary, so for arrays/maps,
		// and try to handle simple cases without it
		marshalled, err := json.Marshal(valueExtracted)
		if err != nil {
			logger.ErrorWithCtx(ctx).Err(err).Msgf("failed to marshal value %v", valueExtracted)
		}
		return fmt.Sprintf(`"%s": %v`, c.ColName, string(marshalled))
	}
}

// ExtractValue returns the value of the column. If it is a pointer, it returns the value of the pointer.
// Care: it's untested how it works with '[]type' or '[]*type'.
func (c *QueryResultCol) ExtractValue() any {
	if c.Value == nil {
		return nil
	}
	v := reflect.ValueOf(c.Value)

	if v.Kind() == reflect.Ptr {
		if v.Elem().Kind() == reflect.Invalid {
			return nil
		}
		return v.Elem().Interface()
	}

	return c.Value
}

func (c *QueryResultCol) isArray() bool {
	if c.Value == nil {
		return false
	}
	v := reflect.ValueOf(c.Value)
	return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
}

func (r *QueryResultRow) String(ctx context.Context) string {
	str := strings.Builder{}
	str.WriteString(util.Indent(1) + "{\n")
	i := 0
	for _, col := range r.Cols {
		// skip internal columns
		if col.ColName == common_table.IndexNameColumn {
			continue
		}

		colStr := col.String(ctx)
		if len(colStr) > 0 {
			if i > 0 {
				str.WriteString(",\n")
			}
			str.WriteString(util.Indent(2) + colStr)
			i++
		}
	}
	str.WriteString("\n" + util.Indent(1) + "}")
	return str.String()
}

// Copy returns a deep copy of the row.
func (r *QueryResultRow) Copy() QueryResultRow {
	newCols := make([]QueryResultCol, len(r.Cols))
	copy(newCols, r.Cols)
	return QueryResultRow{Index: r.Index, Cols: newCols}
}

func (r *QueryResultRow) LastColValue() any {
	return r.Cols[len(r.Cols)-1].Value
}

func (r *QueryResultRow) SecondLastColValue() any {
	return r.Cols[len(r.Cols)-2].Value
}

// SameSubsetOfColumns returns if r and other have the same values for columns with names in colNames
// They are results of the same query, so we can assume that the columns are in the same order.
func (r *QueryResultRow) SameSubsetOfColumns(other *QueryResultRow, colNames []string) bool {
	for i := range min(len(r.Cols), len(other.Cols)) {
		if slices.Contains(colNames, r.Cols[i].ColName) {
			isArray1 := r.Cols[i].isArray()
			isArray2 := other.Cols[i].isArray()

			if !isArray1 && !isArray2 {
				if r.Cols[i].ExtractValue() != other.Cols[i].ExtractValue() {
					return false
				}
			} else if isArray1 && isArray2 {
				if !reflect.DeepEqual(r.Cols[i].Value, other.Cols[i].Value) {
					return false
				}
			} else {
				return false
			}
		}
	}
	return true
}

// firstNColumnsHaveSameValues returns if 2 rows have the same values for the first N columns
func (r *QueryResultRow) firstNColumnsHaveSameValues(other *QueryResultRow, N int) bool {
	for i := 0; i < N; i++ {
		isArray1 := r.Cols[i].isArray()
		isArray2 := other.Cols[i].isArray()

		if !isArray1 && !isArray2 {
			if r.Cols[i].ExtractValue() != other.Cols[i].ExtractValue() {
				return false
			}
		} else if isArray1 && isArray2 {
			if !reflect.DeepEqual(r.Cols[i].Value, other.Cols[i].Value) {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// SplitResultSetIntoBuckets splits ResultSet into buckets, based on the first N + 1 columns
// E.g. if N == 0, we split into buckets based on the first field,
// e.g. [row(1, ...), row(1, ...), row(2, ...), row(2, ...), row(3, ...)] -> [[row(1, ...), row(1, ...)], [row(2, ...), row(2, ...)], [row(3, ...)]]
func SplitResultSetIntoBuckets(ResultSet []QueryResultRow, N int) [][]QueryResultRow {
	if len(ResultSet) == 0 {
		return [][]QueryResultRow{{}}
	}

	lastRow := ResultSet[0]
	buckets := [][]QueryResultRow{{lastRow}}
	for _, row := range ResultSet[1:] {
		if row.firstNColumnsHaveSameValues(&lastRow, N) {
			buckets[len(buckets)-1] = append(buckets[len(buckets)-1], row)
		} else {
			buckets = append(buckets, []QueryResultRow{row})
		}
		lastRow = row
	}
	return buckets
}

func FirstNonNilIndex(rows []QueryResultRow) int {
	for i, row := range rows {
		if row.LastColValue() != nil {
			return i
		}
	}
	return -1
}
