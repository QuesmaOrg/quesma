// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package logger

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ForwarderHandler struct {
	counter atomic.Int32
	barrier *sync.WaitGroup
}

// ServeHTTP is the method that serves as the handler
func (h *ForwarderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reader := io.NopCloser(r.Body)
	body, err := io.ReadAll(reader)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	h.counter.Add(int32(len(body)))
	for i := 0; i < len(body); i++ {
		h.barrier.Done()
	}
}

func startHttpServerF(handler *ForwarderHandler, addr string) {
	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	err := server.ListenAndServe()
	if err != nil {
		fmt.Println("Error:", err)
	}
}

func TestLogForwarder(t *testing.T) {
	const BUFFER_SIZE = 32 * 1024
	const ITERATIONS = 1000
	const INTERVAL = time.Millisecond * 100
	const URL = "http://localhost:8094"
	const LOG_MESSAGE = "log message"
	barrier := &sync.WaitGroup{}
	barrier.Add(len(LOG_MESSAGE) * ITERATIONS)
	handler := &ForwarderHandler{barrier: barrier}
	go startHttpServerF(handler, ":8094")
	logSender := makeLogSender(URL, BUFFER_SIZE, INTERVAL)

	logForwarder := &LogForwarder{logSender: logSender,
		logCh:  make(chan []byte, initialBufferSize),
		ticker: time.NewTicker(INTERVAL),
		sigCh:  make(chan os.Signal),
		doneCh: make(chan struct{})}

	logForwarder.Run()
	logForwarder.TriggerFlush()

	for i := 0; i < ITERATIONS; i++ {
		_, _ = logForwarder.Write([]byte(LOG_MESSAGE))
	}
	barrier.Wait()
	assert.Equal(t, len(LOG_MESSAGE)*ITERATIONS, int(handler.counter.Load()))
}
