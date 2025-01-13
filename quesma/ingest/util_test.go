// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ingest

import (
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TODO change into one test with multiple cases
func TestParseTypeFromShowColumns_1(t *testing.T) {
	typ, name := parseTypeFromShowColumns("String", "field-name")
	assert.Equal(t, clickhouse.NewBaseType("String"), typ)
	assert.Equal(t, "field-name", name)
}

func TestParseTypeFromShowColumns_2(t *testing.T) {
	typ, _ := parseTypeFromShowColumns("Array(String)", "a")
	assert.Equal(t, clickhouse.CompoundType{Name: "Array", BaseType: clickhouse.NewBaseType("String")}, typ)
}

func TestParseTypeFromShowColumns_3(t *testing.T) {
	typ, _ := parseTypeFromShowColumns("Array(Array(String))", "a") // TODO what is this second return value for?
	assert.Equal(t, clickhouse.CompoundType{
		Name: "Array",
		BaseType: clickhouse.CompoundType{
			Name:     "Array",
			BaseType: clickhouse.NewBaseType("String"),
		},
	}, typ)
}

func TestParseTypeFromShowColumnsTuple_1(t *testing.T) {
	typ, _ := parseTypeFromShowColumns("Tuple(a String, b String)", "a")
	mvt, ok := typ.(clickhouse.MultiValueType)
	assert.True(t, ok)
	assert.Equal(t, 2, len(mvt.Cols))
	assert.Equal(t, "a", mvt.Cols[0].Name)
	assert.Equal(t, clickhouse.NewBaseType("String"), mvt.Cols[0].Type)
	assert.Equal(t, "b", mvt.Cols[1].Name)
	assert.Equal(t, clickhouse.NewBaseType("String"), mvt.Cols[1].Type)
}

func TestParseTypeFromShowColumnsTuple_2(t *testing.T) {
	typ, _ := parseTypeFromShowColumns("Tuple(Tuple(a String, b Int64), c String)", "a")
	mvt, ok := typ.(clickhouse.MultiValueType)
	assert.True(t, ok)
	assert.Equal(t, 2, len(mvt.Cols))
	assert.Equal(t, "Tuple", mvt.Cols[0].Name)

	mvt2, ok := mvt.Cols[0].Type.(clickhouse.MultiValueType)
	assert.True(t, ok)
	assert.Equal(t, 2, len(mvt2.Cols))
	assert.Equal(t, "a", mvt2.Cols[0].Name)
	assert.Equal(t, clickhouse.NewBaseType("String"), mvt2.Cols[0].Type)
	assert.Equal(t, "b", mvt2.Cols[1].Name)
	assert.Equal(t, clickhouse.NewBaseType("Int64"), mvt2.Cols[1].Type)

	assert.Equal(t, clickhouse.NewBaseType("String"), mvt.Cols[1].Type)
	assert.Equal(t, "c", mvt.Cols[1].Name)
}
