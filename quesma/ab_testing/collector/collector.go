// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package collector

import (
	"context"
	"quesma/ab_testing"
	"quesma/buildinfo"
	"quesma/logger"
	"quesma/quesma/recovery"
	"time"
)

type ResponseMismatch struct {
	Mismatches string `json:"mismatches"` // JSON array of differences
	// TODO maybe we should keep more human readable list of differences
	IsMismatch      bool   `json:"is_mismatch"`
	Count           int    `json:"count"`
	TopMismatchType string `json:"top_mismatch_type"`
	Message         string `json:"message"`
}

type Collector interface {
	Collect(result ab_testing.Result)
}

// it holds the EnrichedResults of the processing
type EnrichedResults struct {
	ab_testing.Result

	Timestamp       string           `json:"@timestamp"`
	Mismatch        ResponseMismatch `json:"response_mismatch"`
	QuesmaVersion   string           `json:"quesma_version"`
	QuesmaBuildHash string           `json:"quesma_hash"`
}

type pipelineProcessor interface {
	process(in EnrichedResults) (out EnrichedResults, drop bool, err error)
}

type processorErrorMessage struct {
	processor pipelineProcessor
	err       error
}

type InMemoryCollector struct {
	ctx          context.Context
	cancelFunc   context.CancelFunc
	receiveQueue chan ab_testing.Result

	pipeline []pipelineProcessor

	processorErrorQueue chan processorErrorMessage

	healthQueue chan<- ab_testing.HealthMessage
	// add  health state
}

func NewCollector(ctx context.Context, healthQueue chan<- ab_testing.HealthMessage) *InMemoryCollector {

	ctx, cancel := context.WithCancel(ctx)

	// TODO read config here

	// avoid unused struct error
	var _ = &ppPrintFanout{}
	var _ = &mismatchedOnlyFilter{}

	return &InMemoryCollector{
		receiveQueue: make(chan ab_testing.Result, 1000),
		ctx:          ctx,
		cancelFunc:   cancel,
		pipeline: []pipelineProcessor{
			&probabilisticSampler{ratio: 1},
			&deAsyncResponse{},
			&diffTransformer{},
			//&ppPrintFanout{},
			//&mismatchedOnlyFilter{},
			&elasticSearchFanout{
				url:       "http://localhost:8080",
				indexName: "ab_testing_logs",
			},
		},
		healthQueue:         healthQueue,
		processorErrorQueue: make(chan processorErrorMessage, 100),
	}
}

func (r *InMemoryCollector) Stop() {
	r.cancelFunc()
	// stop everything and clean up ASAP

}

func (r *InMemoryCollector) Start() {

	logger.Info().Msg("Starting A/B Results Collector")

	go func() {
		recovery.LogAndHandlePanic(r.ctx, func(err error) {
			r.cancelFunc()
		})
		r.receiveIncomingResults()
	}()

	go func() {
		recovery.LogAndHandlePanic(r.ctx, func(err error) {
			r.cancelFunc()
		})
		r.receiveHealthAndErrorsLoop()
	}()
}

func (r *InMemoryCollector) Collect(data ab_testing.Result) {
	r.receiveQueue <- data
}

// receiveIncomingResults - it processResult incoming results
func (r *InMemoryCollector) receiveIncomingResults() {

	for {
		select {

		case <-r.ctx.Done():
			return

		case msg := <-r.receiveQueue:
			r.processResult(msg)
		}
	}
}

// receiveHealthAndErrorsLoop - it processResult incoming error/health messages
func (r *InMemoryCollector) receiveHealthAndErrorsLoop() {

	errorCount := 0

	sendHealthMessage := func() {
		logger.DebugWithCtx(r.ctx).Msgf("Collector error count: %v", errorCount)
		r.healthQueue <- ab_testing.HealthMessage{
			IsHealthy: errorCount == 0,
		}
	}

	for {
		logger.DebugWithCtx(r.ctx).Msg("Collector control loop cycle")

		select {

		case msg := <-r.processorErrorQueue:
			logger.WarnWithCtx(r.ctx).Msgf("Processor returned an error: %v %v", msg.processor, msg.err)

			errorCount += 1

			sendHealthMessage()
			// TODO add action here
			//
			// gather stats
			// apply rate limiting, back pressure, etc
			// shutdown itself
			//
		case <-r.ctx.Done():
			logger.InfoWithCtx(r.ctx).Msg("Results collector stopping control loop")
			return

		case <-time.After(10 * time.Second):
			sendHealthMessage()
		}
	}
}

func (r *InMemoryCollector) processResult(result ab_testing.Result) {

	// convert raw data to a log line
	res := EnrichedResults{
		Result:          result,
		QuesmaVersion:   buildinfo.Version,
		QuesmaBuildHash: buildinfo.BuildHash,
	}
	res.Timestamp = time.Now().Format(time.RFC3339)

	var err error
	var drop bool

	for _, processor := range r.pipeline {
		if res, drop, err = processor.process(res); err != nil {
			r.processorErrorQueue <- processorErrorMessage{
				processor: processor,
				err:       err,
			}
			return
		}

		if drop {
			return
		}
	}
}
