// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package sender

import (
	"context"
	"quesma/ab_testing"
	"quesma/ab_testing/collector"
	"quesma/logger"
	"quesma/quesma/config"
	"quesma/quesma/recovery"
	"time"
)

type SenderCoordinator struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	facade *sender

	enabled bool
}

func NewSenderCoordinator(cfg config.QuesmaConfiguration) *SenderCoordinator {

	ctx, cancel := context.WithCancel(context.Background())

	return &SenderCoordinator{
		facade:     NewSender(ctx),
		ctx:        ctx,
		cancelFunc: cancel,
		enabled:    false, // TODO this should be read from config
		// add quesma health monitor service here
	}
}

func (c *SenderCoordinator) GetSender() ab_testing.Sender {
	if c.enabled {
		return c.facade
	} else {
		return ab_testing.NewEmptySender()
	}
}

func (c *SenderCoordinator) newInMemoryProcessor(healthQueue chan<- ab_testing.HealthMessage) *collector.InMemoryCollector {
	repo := collector.NewCollector(c.ctx, healthQueue)
	repo.Start()
	return repo
}

func (c *SenderCoordinator) receiveHealthStatusesLoop() {

	var repo *collector.InMemoryCollector
	repoHealthQueue := make(chan ab_testing.HealthMessage)

	updateFacade := func(r ab_testing.Sender) {
		c.facade.controlQueue <- senderControlMessage{
			useCollector: r,
		}
	}

	for {
		logger.InfoWithCtx(c.ctx).Msg("AB Testing Controller Loop")

		if repo == nil {
			logger.InfoWithCtx(c.ctx).Msg("Creating InMemoryRepository")
			repo = c.newInMemoryProcessor(repoHealthQueue)
		}

		// TODO add logic here

		select {
		case <-c.ctx.Done():
			return

		case h := <-repoHealthQueue:

			logger.InfoWithCtx(c.ctx).Msgf("AB Testing Repository Health: %v", h.IsHealthy)

			if !h.IsHealthy {
				updateFacade(nil)

				// we should give a chance to the collector to recover

				logger.InfoWithCtx(c.ctx).Msg("Stopping  InMemoryRepository")
				repo.Stop()
				repo = nil
			} else {
				updateFacade(repo)
			}

		case <-time.After(10 * time.Second):
			// check if collector is still alive
		}
	}
}

func (c *SenderCoordinator) Start() {

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
		c.receiveHealthStatusesLoop()
	}()

}

func (c *SenderCoordinator) Stop() {
	logger.InfoWithCtx(c.ctx).Msg("Stopping AB Testing Controller")
	c.cancelFunc()
}
