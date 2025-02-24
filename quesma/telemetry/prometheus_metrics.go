// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package telemetry

import (
	"github.com/QuesmaOrg/quesma/quesma/v2/core/diag"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

// Define metrics
var (
	ingestionTotalCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "quesma_ingestion_total_entries",
			Help: "Total number of ingestion documents/logs transpiled by Quesma ",
		},
	)

	clickHouseRequestQueryDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "quesma_clickhouse_query_duration_seconds",
			Help:    "Histogram of ClickHouse query duration times",
			Buckets: prometheus.DefBuckets,
		},
	)

	clickHouseRequestIngestDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "quesma_clickhouse_ingest_duration_seconds",
			Help:    "Histogram of ClickHouse ingest duration times",
			Buckets: prometheus.DefBuckets,
		},
	)
)

type ingestionCounterWrapper struct {
	wrapped diag.MultiCounter
}

func newPrometheusIngestionWrapper(wrapped diag.MultiCounter) diag.MultiCounter {
	return &ingestionCounterWrapper{wrapped: wrapped}
}

func (w *ingestionCounterWrapper) Add(key string, value int64) {
	w.wrapped.Add(key, value)
	ingestionTotalCount.Add(float64(value))
}

func (w *ingestionCounterWrapper) AggregateAndReset() diag.MultiCounterStats {
	return w.wrapped.AggregateAndReset()
}

func (w *ingestionCounterWrapper) AggregateTopValuesAndReset() diag.MultiCounterTopValuesStats {
	return w.wrapped.AggregateTopValuesAndReset()
}

type queryDurationWrapper struct {
	histogram prometheus.Histogram
	wrapped   diag.DurationMeasurement
}

func newQueryDurationWrapper(histogram prometheus.Histogram, wrapped diag.DurationMeasurement) diag.DurationMeasurement {
	return &queryDurationWrapper{histogram: histogram, wrapped: wrapped}
}

func (w *queryDurationWrapper) Begin() diag.Span {
	return newSpanDurationWrapper(w.histogram, w.wrapped.Begin())
}

func (w *queryDurationWrapper) AggregateAndReset() diag.DurationStats {
	return w.wrapped.AggregateAndReset()
}

type spanDurationWrapper struct {
	histogram prometheus.Histogram
	wrapped   diag.Span
}

func newSpanDurationWrapper(histogram prometheus.Histogram, wrapped diag.Span) diag.Span {
	return &spanDurationWrapper{histogram: histogram, wrapped: wrapped}
}

func (w *spanDurationWrapper) End(err error) time.Duration {
	duration := w.wrapped.End(err)
	w.histogram.Observe(duration.Seconds())
	return duration
}

func init() {
	// Register metrics with Prometheus
	prometheus.MustRegister(ingestionTotalCount)
	prometheus.MustRegister(clickHouseRequestQueryDuration)
	prometheus.MustRegister(clickHouseRequestIngestDuration)
}
