// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package database_common

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetDateTimeType(t *testing.T) {
	ctx := context.Background()
	table := Table{
		Name: "table",
		Cols: map[string]*Column{
			"timestamp1":    {Name: "timestamp1", Type: NewBaseType("DateTime")},
			"timestamp2":    {Name: "timestamp2", Type: NewBaseType("DateTime('UTC')")},
			"timestamp64_1": {Name: "timestamp64_1", Type: NewBaseType("DateTime64")},
			"timestamp64_2": {Name: "timestamp64_2", Type: NewBaseType("DateTime64(3, 'UTC')")},
			"datetime1":     {Name: "datetime1", Type: NewBaseType("datetime")},
			"date1":         {Name: "date1", Type: NewBaseType("date")},
		},
		Config: NewChTableConfigTimestampStringAttr(),
	}
	assert.Equal(t, DateTime, table.GetDateTimeType(ctx, "timestamp1", true))
	assert.Equal(t, DateTime, table.GetDateTimeType(ctx, "timestamp2", true))
	assert.Equal(t, DateTime64, table.GetDateTimeType(ctx, "timestamp64_1", true))
	assert.Equal(t, DateTime64, table.GetDateTimeType(ctx, "timestamp64_2", true))
	assert.Equal(t, DateTime64, table.GetDateTimeType(ctx, timestampFieldName, true)) // default, created by us
	assert.Equal(t, DateTime64, table.GetDateTimeType(ctx, "datetime1", true))
	assert.Equal(t, DateTime, table.GetDateTimeType(ctx, "date1", true))
	assert.Equal(t, Invalid, table.GetDateTimeType(ctx, "non-existent", false))
}
