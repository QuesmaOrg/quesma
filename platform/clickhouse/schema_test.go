// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetDateTimeType(t *testing.T) {
	ctx := context.Background()
	table, err := NewTable(`CREATE TABLE table (
		"timestamp1" DateTime,
		"timestamp2" DateTime('UTC'),
		"timestamp64_1" DateTime64,
		"timestamp64_2" DateTime64(3, 'UTC') ) ENGINE = Memory`, NewChTableConfigTimestampStringAttr())
	assert.NoError(t, err)
	assert.Equal(t, DateTime, table.GetDateTimeType(ctx, "timestamp1", true))
	assert.Equal(t, DateTime, table.GetDateTimeType(ctx, "timestamp2", true))
	assert.Equal(t, DateTime64, table.GetDateTimeType(ctx, "timestamp64_1", true))
	assert.Equal(t, DateTime64, table.GetDateTimeType(ctx, "timestamp64_2", true))
	assert.Equal(t, DateTime64, table.GetDateTimeType(ctx, timestampFieldName, true)) // default, created by us
	assert.Equal(t, Invalid, table.GetDateTimeType(ctx, "non-existent", false))
}
