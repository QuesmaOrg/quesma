// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIngestValidation(t *testing.T) {
	var value interface{}
	{
		value = 1
		assert.Equal(t, "UInt64", getTypeName(value))
	}
	{
		value = 1.1
		assert.Equal(t, "Float64", getTypeName(value))
	}
	{
		value = -1
		assert.Equal(t, "Int64", getTypeName(value))
	}
	{
		value = "string"
		assert.Equal(t, "String", getTypeName(value))
	}
	{
		value = true
		assert.Equal(t, "Bool", getTypeName(value))
	}
	{
		value = []interface{}{1}
		assert.Equal(t, "Array(UInt64)", getTypeName(value))
	}
	{
		value = []interface{}{-1}
		assert.Equal(t, "Array(Int64)", getTypeName(value))
	}
	{
		value = [][]interface{}{{-1}}
		assert.Equal(t, "Array(Array(Int64))", getTypeName(value))
	}
	{
		value = [][][]interface{}{{{-1}}}
		assert.Equal(t, "Array(Array(Array(Int64)))", getTypeName(value))
	}

}
