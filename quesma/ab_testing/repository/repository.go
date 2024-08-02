// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package repository

import (
	"context"
	"quesma/ab_testing"
	"quesma/logger"
	"quesma/quesma/recovery"
	"time"
)

type Diff struct {
	BodyDiff string `json:"body_diff"`
	IsDiff   bool   `json:"is_diff"`
}

// it holds the Data of the processing

type Data struct {
	ab_testing.Result

	Timestamp string `json:"@timestamp"`
	Diff      Diff   `json:"diff"`
}

type processor interface {
	process(in Data) (out Data, drop bool, err error)
}

type processorErrorMessage struct {
	processor processor
	err       error
}

type ResultsRepositoryImpl struct {
	ctx          context.Context
	cancelFunc   context.CancelFunc
	receiveQueue chan ab_testing.Result

	pipeline []processor

	processorErrorQueue chan processorErrorMessage

	healthQueue chan<- ab_testing.HealthMessage
	// add  health state
}

func NewResultsRepository(ctx context.Context, healthQueue chan<- ab_testing.HealthMessage) *ResultsRepositoryImpl {

	ctx, cancel := context.WithCancel(ctx)

	// TODO read config here

	return &ResultsRepositoryImpl{
		receiveQueue: make(chan ab_testing.Result, 1000),
		ctx:          ctx,
		cancelFunc:   cancel,
		pipeline: []processor{
			&probabilisticSampler{ratio: 1},
			&diffTransformer{},
			//&ppPrintFanout{},
			&elasticSearchFanout{
				url:       "http://localhost:8080",
				indexName: "ab_testing_logs",
			},
		},
		healthQueue:         healthQueue,
		processorErrorQueue: make(chan processorErrorMessage, 100),
	}
}

func (r *ResultsRepositoryImpl) Stop() {
	r.cancelFunc()
	// stop everything and clean up ASAP

}

func (r *ResultsRepositoryImpl) Start() {

	logger.Info().Msg("Starting A/B Results Repository")

	go func() {
		recovery.LogAndHandlePanic(r.ctx, func(err error) {
			r.cancelFunc()
		})
		r.loop()
	}()

	go func() {
		recovery.LogAndHandlePanic(r.ctx, func(err error) {
			r.cancelFunc()
		})
		r.controlLoop()
	}()
}

func (r *ResultsRepositoryImpl) Store(data ab_testing.Result) {
	r.receiveQueue <- data
}

// loop - it process incoming results
func (r *ResultsRepositoryImpl) loop() {

	for {
		select {

		case <-r.ctx.Done():
			return

		case msg := <-r.receiveQueue:
			r.process(msg)
		}
	}
}

// controlLoop - it process incoming error/health messages
func (r *ResultsRepositoryImpl) controlLoop() {

	errorCount := 0

	sendHealthMessage := func() {
		logger.InfoWithCtx(r.ctx).Msgf("Results Repository Error Count: %v", errorCount)
		r.healthQueue <- ab_testing.HealthMessage{
			IsHealthy: errorCount == 0,
		}
	}

	for {
		logger.InfoWithCtx(r.ctx).Msg("Results Repository Control Loop cycle")

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
			logger.InfoWithCtx(r.ctx).Msg("Results Repository stopping control loop")
			return

		case <-time.After(10 * time.Second):
			sendHealthMessage()
		}
	}
}

func (r *ResultsRepositoryImpl) process(result ab_testing.Result) {

	// convert raw data to a log line
	msg := Data{
		Result: result,
	}
	msg.Timestamp = time.Now().Format(time.RFC3339)

	var err error
	var drop bool

	for _, processor := range r.pipeline {
		logger.InfoWithCtx(r.ctx).Msgf("Processing with %v", processor)
		if msg, drop, err = processor.process(msg); err != nil {
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
