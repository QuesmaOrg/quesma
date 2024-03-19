package telemetry

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDurationMeasurement_Aggregate(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	measurement := newDurationMeasurement(ctx)

	for i := 0; i < 100; i++ {
		measurement.ingestSample(durationSample{ok: true, elapsed: float64(i) / 2})
	}

	for i := 0; i < 10; i++ {
		measurement.ingestSample(durationSample{ok: false, elapsed: 100})
	}

	stats := measurement.Aggregate()

	assert.Equal(t, int64(100), stats.Count)
	assert.Equal(t, int64(10), stats.Failed)
	assert.Equal(t, float64(24.75), stats.Avg)

	assert.Equal(t, int64(97), stats.OverThresholds["1"])
	assert.Equal(t, int64(89), stats.OverThresholds["5"])
	assert.Equal(t, int64(79), stats.OverThresholds["10"])
	assert.Equal(t, int64(39), stats.OverThresholds["30"])
	assert.Equal(t, int64(0), stats.OverThresholds["60"])

	assert.Equal(t, float32(25.0), stats.Percentiles["50"])

}

func TestDurationMeasurement_Percentiles(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	measurement := newDurationMeasurement(ctx)

	for i := 0; i < percentileSamplePoolSize*2; i++ {
		measurement.ingestSample(durationSample{ok: true, elapsed: float64(i % 100)})
	}

	stats := measurement.Aggregate()

	assert.Equal(t, float32(50.0), stats.Percentiles["50"])

}

func TestDurationMeasurement_Percentiles_no_samples(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	measurement := newDurationMeasurement(ctx)

	stats := measurement.Aggregate()

	assert.Equal(t, float32(0.0), stats.Percentiles["50"])
}

func TestDurationMeasurement_Percentiles_single_sample(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	measurement := newDurationMeasurement(ctx)

	measurement.ingestSample(durationSample{ok: true, elapsed: float64(1)})

	stats := measurement.Aggregate()

	assert.Equal(t, float32(1.0), stats.Percentiles["50"])
}
