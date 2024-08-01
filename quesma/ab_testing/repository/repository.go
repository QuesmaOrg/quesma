// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package repository

import (
	"context"
	"fmt"
	"quesma/ab_testing"
	"quesma/quesma/recovery"
	"time"
)

type processor interface {
	process(in ab_testing.Result) (out ab_testing.Result, drop bool, err error)
}

type ResultsRepositoryImpl struct {
	ctx          context.Context
	cancelFunc   context.CancelFunc
	receiveQueue chan ab_testing.Result

	pipeline []processor

	// add  health state
}

func (r *ResultsRepositoryImpl) Stop() {
	r.cancelFunc()
	// stop everything and clean up ASAP

}

func (r *ResultsRepositoryImpl) Start() {
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
		recovery.LogPanic()
	}()
}

func NewResultsRepository() *ResultsRepositoryImpl {

	ctx, cancel := context.WithCancel(context.Background())

	return &ResultsRepositoryImpl{
		receiveQueue: make(chan ab_testing.Result, 1000),
		ctx:          ctx,
		cancelFunc:   cancel,
	}
}

func (r *ResultsRepositoryImpl) Store(data ab_testing.Result) {
	r.receiveQueue <- data
}

func (r *ResultsRepositoryImpl) loop() {

	select {

	case <-r.ctx.Done():
		return

	case msg := <-r.receiveQueue:
		r.process(msg)
	}
}

func (r *ResultsRepositoryImpl) controlLoop() {
	select {

	// gather metrics

	// apply rate limiting, back pressure, etc

	case <-r.ctx.Done():
		return

	case <-time.After(10 * time.Second):
	}
}

func (r *ResultsRepositoryImpl) process(msg ab_testing.Result) {

	var err error
	var drop bool
	for _, processor := range r.pipeline {
		if msg, drop, err = processor.process(msg); err != nil {
			fmt.Println("error processing message", err)
			return
		}

		if drop {
			return
		}
	}
}
