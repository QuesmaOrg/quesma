package telemetry

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMultiCounter_Add(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := NewMultiCounter(ctx)
	mc.Add("key1", 1)
	mc.Add("key2", 2)
	mc.Add("key1", 3)

	time.Sleep(1 * time.Second)

	stats := mc.Aggregate()

	assert.Equal(t, int64(4), stats["key1"])
	assert.Equal(t, int64(2), stats["key2"])
	assert.Equal(t, 2, len(stats))
}
