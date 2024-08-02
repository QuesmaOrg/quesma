// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package controller

import (
	"context"
	"quesma/ab_testing"
	"quesma/logger"
	"quesma/quesma/recovery"
)

type facadeControlMessage struct {
	newDelegate ab_testing.ResultsRepository
}

type facade struct {
	ctx          context.Context
	delegate     ab_testing.ResultsRepository
	queue        chan ab_testing.Result
	controlQueue chan facadeControlMessage
}

func NewFacade(ctx context.Context) *facade {

	return &facade{
		ctx:          ctx,
		delegate:     nil,
		queue:        make(chan ab_testing.Result, 10),
		controlQueue: make(chan facadeControlMessage, 10),
	}

}

func (f *facade) Start() {

	go func() {
		recovery.LogPanic()

		for {
			select {

			case ctrl := <-f.controlQueue:

				if f.delegate != ctrl.newDelegate {
					logger.InfoWithCtx(f.ctx).Msgf("Facade: New repository: %s ", ctrl.newDelegate)
					f.delegate = ctrl.newDelegate
				}

				f.delegate = ctrl.newDelegate

			case result := <-f.queue:

				if f.delegate != nil {
					f.delegate.Store(result)
				} else {
					// no repository, just drop results
				}

			case <-f.ctx.Done():
				return
			}
		}
	}()
}

func (f *facade) Store(data ab_testing.Result) {
	f.queue <- data
}
