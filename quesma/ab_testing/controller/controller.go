// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package controller

import (
	"context"
	"quesma/ab_testing"
	"quesma/ab_testing/repository"
	"quesma/logger"
	"quesma/quesma/recovery"
	"time"
)

type ABTestingController struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	facade     *facade

	repository *repository.ResultsRepositoryImpl
}

func NewABTestingController() *ABTestingController {
	return &ABTestingController{
		facade:     &facade{},
		repository: repository.NewResultsRepository(),
		// add quesma health monitor service here
	}
}

func (c *ABTestingController) Client() ab_testing.ResultsRepository {
	return c.facade
}

func (c *ABTestingController) loop() {

	for {
		logger.InfoWithCtx(c.ctx).Msg("AB Testing Controller Loop")
		// add logic here
		// start/stop  the inMemoryRepository repository

		// sets the delegate if the in memory is healthy

		// disable facade if quesma is healthy

		c.facade.delegate = c.repository

		select {
		case <-c.ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}
}

func (c *ABTestingController) Start() {

	ctx, cancel := context.WithCancel(context.Background())

	logger.InfoWithCtx(ctx).Msg("Starting AB Testing Controller")

	c.ctx = ctx
	c.cancelFunc = cancel

	go func() {
		recovery.LogAndHandlePanic(c.ctx, func(err error) {
			c.cancelFunc()
		})
		c.loop()
	}()

	// c.inMemoryRepository.Start()
}

func (c *ABTestingController) Stop() {
	logger.InfoWithCtx(c.ctx).Msg("Stopping AB Testing Controller")
	c.cancelFunc()
}
