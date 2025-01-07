// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"context"
	"reflect"
)

type QueryProcessor struct {
	ctx context.Context
}

func NewQueryProcessor(ctx context.Context) QueryProcessor {
	return QueryProcessor{ctx: ctx}
}

// SameSubsetOfColumns returns if row1 and row2 have the same values for columns with names in colNames
// They are results of the same query, so we can assume that the columns are in the same order (and same length)
func (qp *QueryProcessor) SameSubsetOfColumns(row1, row2 QueryResultRow, colNames []string) bool {
	isArray := func(val interface{}) bool {
		if val == nil {
			return false
		}
		v := reflect.ValueOf(val)
		return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
	}

	for i := range min(len(row1.Cols), len(row2.Cols)) {
		val1 := row1.Cols[i].Value
		val2 := row2.Cols[i].Value
		isArray1 := isArray(val1)
		isArray2 := isArray(val2)

		if !isArray1 && !isArray2 {
			if row1.Cols[i].ExtractValue(qp.ctx) != row2.Cols[i].ExtractValue(qp.ctx) {
				return false
			}
		} else if isArray1 && isArray2 {
			if !reflect.DeepEqual(val1, val2) {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// sameFirstNColumns returns if row1 and row2 have the same values for the first N columns
func (qp *QueryProcessor) sameFirstNColumns(row1, row2 QueryResultRow, N int) bool {

	isArray := func(val interface{}) bool {
		if val == nil {
			return false
		}
		v := reflect.ValueOf(val)
		return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
	}

	for i := 0; i < N; i++ {
		val1 := row1.Cols[i].Value
		val2 := row2.Cols[i].Value
		isArray1 := isArray(val1)
		isArray2 := isArray(val2)

		if !isArray1 && !isArray2 {
			if row1.Cols[i].ExtractValue(qp.ctx) != row2.Cols[i].ExtractValue(qp.ctx) {
				return false
			}
		} else if isArray1 && isArray2 {
			if !reflect.DeepEqual(val1, val2) {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// SplitResultSetIntoBuckets splits ResultSet into buckets, based on the first N fields
// E.g. if level == 0, we split into buckets based on the first field,
// e.g. [row(1, ...), row(1, ...), row(2, ...), row(2, ...), row(3, ...)] -> [[row(1, ...), row(1, ...)], [row(2, ...), row(2, ...)], [row(3, ...)]]
func (qp *QueryProcessor) SplitResultSetIntoBuckets(ResultSet []QueryResultRow, N int) [][]QueryResultRow {
	if len(ResultSet) == 0 {
		return [][]QueryResultRow{{}}
	}

	lastRow := ResultSet[0]
	buckets := [][]QueryResultRow{{lastRow}}
	for _, row := range ResultSet[1:] {
		if qp.sameFirstNColumns(row, lastRow, N) {
			buckets[len(buckets)-1] = append(buckets[len(buckets)-1], row)
		} else {
			buckets = append(buckets, []QueryResultRow{row})
		}
		lastRow = row
	}
	return buckets
}
