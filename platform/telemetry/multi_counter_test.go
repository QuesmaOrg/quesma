// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package telemetry

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMultiCounter_Add(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	mc := NewMultiCounter(ctx, nil)
	mc.(*multiCounter).ingestDoneCh = make(chan interface{}, 20)

	mc.Add("key1", 1)
	mc.Add("key2", 2)
	mc.Add("key1", 3)

	// wait for all the ingests to complete
	for range 3 {
		select {
		case <-mc.(*multiCounter).ingestDoneCh:
			// do nothing
		case <-ctx.Done():
			t.Errorf("ingest did not complete in time")
		}
	}

	stats := mc.AggregateAndReset()

	assert.Equal(t, int64(4), stats["key1"])
	assert.Equal(t, int64(2), stats["key2"])
	assert.Equal(t, 2, len(stats))

	stats2 := mc.AggregateAndReset()
	assert.Empty(t, stats2)
}
