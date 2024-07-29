// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTypeName(t *testing.T) {
	values := make(map[string][]interface{})
	values["UInt64"] = []interface{}{1}
	values["Float64"] = []interface{}{1.1}
	values["Int64"] = []interface{}{-1}
	values["String"] = []interface{}{"string"}
	values["Bool"] = []interface{}{true}
	values["Array(UInt64)"] = []interface{}{[]interface{}{1}}
	values["Array(Int64)"] = []interface{}{[]interface{}{-1}}
	values["Array(Array(Int64))"] = []interface{}{[][]interface{}{{-1}}}
	values["Array(Array(Array(Int64)))"] = []interface{}{[][][]interface{}{{{-1}}}}
	for typeName, values := range values {
		for _, value := range values {
			t.Run(typeName, func(t *testing.T) {
				assert.NotNil(t, value)
				assert.Equal(t, typeName, getTypeName(value))
			})
		}
	}
}
