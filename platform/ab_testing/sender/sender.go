// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package sender

import (
	"context"
	"github.com/QuesmaOrg/quesma/platform/ab_testing"
	"github.com/QuesmaOrg/quesma/platform/ab_testing/collector"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/recovery"
)

type senderControlMessage struct {
	useCollector collector.Collector
}

// sender sends results to the collector if available. This implementation is managed by SenderCoordinator.
type sender struct {
	ctx          context.Context
	collector    collector.Collector // collector has the same interfaces
	queue        chan ab_testing.Result
	controlQueue chan senderControlMessage
}

func newSender(ctx context.Context) *sender {

	return &sender{
		ctx:          ctx,
		collector:    nil,
		queue:        make(chan ab_testing.Result, 10),
		controlQueue: make(chan senderControlMessage, 10),
	}

}

func (f *sender) Start() {

	go func() {
		defer recovery.LogPanic()

		for {
			select {

			case ctrl := <-f.controlQueue:

				if f.collector != ctrl.useCollector {
					logger.InfoWithCtx(f.ctx).Msgf("Sender: New collector: %v", ctrl.useCollector)
					f.collector = ctrl.useCollector
				}

				f.collector = ctrl.useCollector

			case result := <-f.queue:

				if f.collector != nil {
					f.collector.Collect(result)
				}

			case <-f.ctx.Done():
				return
			}
		}
	}()
}

func (f *sender) Send(data ab_testing.Result) {
	f.queue <- data
}
