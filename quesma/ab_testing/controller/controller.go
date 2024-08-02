// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package controller

import (
	"context"
	"quesma/ab_testing"
	"quesma/ab_testing/repository"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"time"
)

type ABTestingController struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	facade *facade

	enabled bool
}

func NewABTestingController(cfg config.QuesmaConfiguration) *ABTestingController {

	ctx, cancel := context.WithCancel(context.Background())

	return &ABTestingController{
		facade:     NewFacade(ctx),
		ctx:        ctx,
		cancelFunc: cancel,
		enabled:    false, // TODO this should be read from config
		// add quesma health monitor service here
	}
}

func (c *ABTestingController) Client() ab_testing.ResultsRepository {
	if c.enabled {
		return c.facade
	} else {
		return ab_testing.NewEmptyResultsRepository()
	}
}

func (c *ABTestingController) newInMemoryRepository(healthQueue chan<- ab_testing.HealthMessage) *repository.ResultsRepositoryImpl {
	repo := repository.NewResultsRepository(c.ctx, healthQueue)
	repo.Start()
	return repo
}

func (c *ABTestingController) loop() {

	var repo *repository.ResultsRepositoryImpl
	repoHealthQueue := make(chan ab_testing.HealthMessage)

	updateFacade := func(r ab_testing.ResultsRepository) {
		c.facade.controlQueue <- facadeControlMessage{
			newDelegate: r,
		}
	}

	for {
		logger.InfoWithCtx(c.ctx).Msg("AB Testing Controller Loop")

		if repo == nil {
			logger.InfoWithCtx(c.ctx).Msg("Creating InMemoryRepository")
			repo = c.newInMemoryRepository(repoHealthQueue)
		}

		// TODO add logic here

		select {
		case <-c.ctx.Done():
			return

		case h := <-repoHealthQueue:

			logger.InfoWithCtx(c.ctx).Msgf("AB Testing Repository Health: %v", h.IsHealthy)

			if !h.IsHealthy {
				updateFacade(nil)

				// we should give a chance to the repository to recover

				logger.InfoWithCtx(c.ctx).Msg("Stopping  InMemoryRepository")
				repo.Stop()
				repo = nil
			} else {
				updateFacade(repo)
			}

		case <-time.After(10 * time.Second):
			// check if repository is still alive
		}
	}
}

func (c *ABTestingController) Start() {

	if !c.enabled {
		logger.InfoWithCtx(c.ctx).Msg("AB Testing Controller is disabled")
		return
	}

	logger.InfoWithCtx(c.ctx).Msg("Starting AB Testing Controller")

	c.facade.Start()

	go func() {
		recovery.LogAndHandlePanic(c.ctx, func(err error) {
			c.cancelFunc()
		})
		c.loop()
	}()

}

func (c *ABTestingController) Stop() {
	logger.InfoWithCtx(c.ctx).Msg("Stopping AB Testing Controller")
	c.cancelFunc()
}
