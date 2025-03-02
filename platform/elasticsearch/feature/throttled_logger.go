// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package feature

import (
	"github.com/QuesmaOrg/quesma/platform/logger"
	"sync"
	"time"
)

const (
	// maxMessages is the maximum number of messages that can be stored in the throttled logger
	maxMessages      = 1000
	evictionInterval = 5 * time.Minute
)

type ThrottledLogger struct {
	m         sync.Mutex
	messages  map[string]bool
	messageCh chan string
	done      chan struct{}
}

func NewThrottledLogger() *ThrottledLogger {
	tl := &ThrottledLogger{
		messageCh: make(chan string, 100),
		done:      make(chan struct{}),
		messages:  make(map[string]bool),
	}

	go tl.run()

	return tl
}

func (tl *ThrottledLogger) ingest(message string) {
	tl.m.Lock()
	defer tl.m.Unlock()

	// this is just in case the map grows too much
	if len(tl.messages) > maxMessages {
		tl.evictInternal()
	}

	if _, ok := tl.messages[message]; !ok {
		logger.Warn().Msg(message)
		tl.messages[message] = true
	}
}

func (tl *ThrottledLogger) Stop() {
	close(tl.done)
}

func (tl *ThrottledLogger) evictInternal() {
	// rudimentary eviction
	tl.messages = make(map[string]bool)
}

func (tl *ThrottledLogger) evict() {
	tl.m.Lock()
	defer tl.m.Unlock()
	tl.evictInternal()
}

func (tl *ThrottledLogger) run() {

	ticker := time.NewTicker(evictionInterval)

	for {
		select {
		case msg := <-tl.messageCh:
			tl.ingest(msg)
		case <-ticker.C:
			tl.evict()
		case <-tl.done:
			ticker.Stop()
			return
		}
	}
}

func (tl *ThrottledLogger) Log(msg string) {
	tl.messageCh <- msg
}
