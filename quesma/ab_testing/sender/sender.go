// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package sender

import (
	"context"
	"quesma/ab_testing"
	"quesma/logger"
	"quesma/quesma/recovery"
)

type senderControlMessage struct {
	useCollector ab_testing.Sender
}

type sender struct {
	ctx          context.Context
	collector    ab_testing.Sender
	queue        chan ab_testing.Result
	controlQueue chan senderControlMessage
}

func NewSender(ctx context.Context) *sender {

	return &sender{
		ctx:          ctx,
		collector:    nil,
		queue:        make(chan ab_testing.Result, 10),
		controlQueue: make(chan senderControlMessage, 10),
	}

}

func (f *sender) Start() {

	go func() {
		recovery.LogPanic()

		for {
			select {

			case ctrl := <-f.controlQueue:

				if f.collector != ctrl.useCollector {
					logger.InfoWithCtx(f.ctx).Msgf("Facade: New collector: %s ", ctrl.useCollector)
					f.collector = ctrl.useCollector
				}

				f.collector = ctrl.useCollector

			case result := <-f.queue:

				if f.collector != nil {
					f.collector.Send(result)
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
