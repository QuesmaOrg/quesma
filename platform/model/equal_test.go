// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPartlyImplementedIsEqual(t *testing.T) {
	// floor("bytes"/100.000000)*100.000000
	floorBytes := NewInfixExpr(NewFunction("floor",
		NewInfixExpr(NewColumnRef("bytes"), "/", NewLiteral("100.000000"))),
		"*", NewLiteral("100.000000"))

	floorBytesAliased := NewAliasedExpr(floorBytes, "floor_bytes")
	assert.True(t, PartlyImplementedIsEqual(floorBytes, floorBytesAliased))

	// toInt64(toUnixTimestamp64Milli("@timestamp") / 30000)
	dateRange := NewFunction("toInt64", NewInfixExpr(
		NewFunction("toUnixTimestamp64Milli", NewColumnRef("@timestamp")), "/", NewLiteral("30000")))
	dateRangeAliased := NewAliasedExpr(dateRange, "date_range")
	assert.True(t, PartlyImplementedIsEqual(dateRange, dateRangeAliased))

	// column ref
	columnRefA := NewColumnRef("bytes")
	columnRefB := NewColumnRef("bytes")
	assert.True(t, PartlyImplementedIsEqual(columnRefA, columnRefB))

	// negative column ref
	columnRefC := NewColumnRef("response_bytes")
	columnRefD := NewColumnRef("response")
	assert.False(t, PartlyImplementedIsEqual(columnRefC, columnRefD))

	// negative shape
	assert.False(t, PartlyImplementedIsEqual(floorBytes, dateRange))
}
