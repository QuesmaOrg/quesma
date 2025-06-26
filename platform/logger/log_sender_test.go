// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package logger

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type Handler struct {
	counter atomic.Int32
	barrier *sync.WaitGroup
}

// ServeHTTP is the method that serves as the handler
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer h.barrier.Done()
	reader := io.NopCloser(r.Body)
	body, err := io.ReadAll(reader)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	h.counter.Add(int32(len(body)))
}

func startHttpServer(handler *Handler, addr string) {
	server := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	err := server.ListenAndServe()
	if err != nil {
		fmt.Println("Error:", err)
	}
}

func makeLogSender(urlStr string, bufferSize int, interval time.Duration) LogSender {
	urlString, _ := url.Parse(urlStr)
	return LogSender{
		Url:          urlString,
		LogBuffer:    make([]byte, 0, bufferSize),
		LastSendTime: time.Now(),
		Interval:     interval,
		httpClient:   &http.Client{},
	}
}

func TestLogSenderFlush(t *testing.T) {
	const BUFFER_SIZE = 12 * 1024
	const ITERATIONS = 1000
	const INTERVAL = time.Minute
	const URL = "http://localhost:8090"
	barrier := &sync.WaitGroup{}
	barrier.Add(1)
	handler := &Handler{barrier: barrier}
	go startHttpServer(handler, ":8090")
	logSender := makeLogSender(URL, BUFFER_SIZE, INTERVAL)
	sendCounter := 0
	for j := 0; j < ITERATIONS; j++ {
		logMessage := "log message"
		sendCounter += len(logMessage)
		result := logSender.EatLogMessage([]byte(logMessage))
		assert.Equal(t, true, result.bufferLengthCondition)
		assert.Equal(t, true, result.timeCondition)
	}
	assert.Equal(t, 0, int(handler.counter.Load()))
	_ = logSender.FlushLogs()
	barrier.Wait()
	assert.Equal(t, sendCounter, int(handler.counter.Load()))
}

func TestLogSenderSmallBuffer(t *testing.T) {
	const BUFFER_SIZE = 8
	const ITERATIONS = 1000
	const INTERVAL = time.Minute
	const URL = "http://localhost:8091"
	const LOG_MESSAGE = "log message"
	barrier := &sync.WaitGroup{}
	barrier.Add(0) // all messages will be dropped
	handler := &Handler{barrier: barrier}
	go startHttpServer(handler, ":8091")

	logSender := makeLogSender(URL, BUFFER_SIZE, INTERVAL)
	sendCounter := 0
	for j := 0; j < ITERATIONS; j++ {
		sendCounter += len(LOG_MESSAGE)
		result := logSender.EatLogMessage([]byte(LOG_MESSAGE))
		assert.Equal(t, false, result.bufferLengthCondition)
		assert.Equal(t, true, result.timeCondition)
	}
	barrier.Wait()
	assert.Equal(t, int(handler.counter.Load()), 0)
}

func TestLogSenderSmallElapsedTime(t *testing.T) {
	const BUFFER_SIZE = 32 * 1024
	const ITERATIONS = 1000
	const INTERVAL = time.Nanosecond
	const URL = "http://localhost:8092"
	const LOG_MESSAGE = "log message"
	barrier := &sync.WaitGroup{}
	barrier.Add(ITERATIONS)
	handler := &Handler{barrier: barrier}
	go startHttpServer(handler, ":8092")

	logSender := makeLogSender(URL, BUFFER_SIZE, INTERVAL)
	sendCounter := 0
	for j := 0; j < ITERATIONS; j++ {
		sendCounter += len(LOG_MESSAGE)
		result := logSender.EatLogMessage([]byte(LOG_MESSAGE))
		assert.Equal(t, true, result.bufferLengthCondition)
		assert.Equal(t, false, result.timeCondition)
	}
	barrier.Wait()
	assert.Equal(t, sendCounter, int(handler.counter.Load()))
}
