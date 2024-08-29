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

// SenderCoordinator - manages sender and in memory collector.
type SenderCoordinator struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	sender *sender // sender managed by this coordinator

	enabled bool
}

func NewSenderCoordinator(cfg *config.QuesmaConfiguration) *SenderCoordinator {

	ctx, cancel := context.WithCancel(context.Background())

	return &SenderCoordinator{
		sender:     newSender(ctx),
		ctx:        ctx,
		cancelFunc: cancel,
		enabled:    true, // TODO this should be read from config
		// add quesma health monitor service here
	}
}

func (c *SenderCoordinator) GetSender() ab_testing.Sender {
	if c.enabled {
		return c.sender
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

	var inMemoryCollector *collector.InMemoryCollector
	repoHealthQueue := make(chan ab_testing.HealthMessage)

	senderUseCollector := func(r collector.Collector) {
		c.sender.controlQueue <- senderControlMessage{
			useCollector: r,
		}
	}

	for {
		if inMemoryCollector == nil {
			logger.InfoWithCtx(c.ctx).Msg("Creating InMemoryCollector")
			inMemoryCollector = c.newInMemoryProcessor(repoHealthQueue)
		}

		// TODO add logic here

		select {
		case <-c.ctx.Done():
			return

		case h := <-repoHealthQueue:

			logger.DebugWithCtx(c.ctx).Msgf("A/B Testing Collector Health: %v", h.IsHealthy)

			if !h.IsHealthy {
				senderUseCollector(nil)

				// we should give a chance to the collector to recover

				logger.InfoWithCtx(c.ctx).Msg("Stopping  InMemoryCollector")
				inMemoryCollector.Stop()
				inMemoryCollector = nil
			} else {
				senderUseCollector(inMemoryCollector)
			}

		case <-time.After(10 * time.Second):
			// check if collector is still alive
		}
	}
}

func (c *SenderCoordinator) Start() {

	if !c.enabled {
		logger.InfoWithCtx(c.ctx).Msg("A/B Testing Controller is disabled")
		return
	}

	logger.InfoWithCtx(c.ctx).Msg("Starting A/B Testing Coordinator")

	c.sender.Start()

	go func() {
		recovery.LogAndHandlePanic(c.ctx, func(err error) {
			c.cancelFunc()
		})
		c.receiveHealthStatusesLoop()
	}()

}

func (c *SenderCoordinator) Stop() {
	logger.InfoWithCtx(c.ctx).Msg("Stopping A/B Testing Controller")
	c.cancelFunc()
}
