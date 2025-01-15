// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package logger

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/telemetry/headers"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type LogSender struct {
	Url          *url.URL
	ClientId     string
	LogBuffer    []byte
	LastSendTime time.Time
	Interval     time.Duration
	httpClient   *http.Client
}

func (logSender *LogSender) EatLogMessage(msg []byte) struct {
	bufferLengthCondition bool
	timeCondition         bool
	Err                   error
} {
	var err error
	elapsed := time.Since(logSender.LastSendTime)
	timeCondition := elapsed < logSender.Interval
	bufferLengthCondition := len(logSender.LogBuffer)+len(msg) < cap(logSender.LogBuffer)
	// Buffer logs if above conditions are met
	// e.g buffer is not full and time since last send is less than interval
	if bufferLengthCondition && timeCondition {
		logSender.LogBuffer = append(logSender.LogBuffer, msg...)
	} else {
		addedBefore := false
		if !bufferLengthCondition && len(logSender.LogBuffer) == 0 { // msg longer than buffer, let's cut it
			cutMark := []byte("...\n")
			charToCut := len(msg) + len(cutMark) - cap(logSender.LogBuffer)
			if charToCut < len(msg) {
				msgCut := msg[:len(msg)-charToCut]
				logSender.LogBuffer = append(logSender.LogBuffer, msgCut...)
				logSender.LogBuffer = append(logSender.LogBuffer, cutMark...)
				addedBefore = true
			}
		} else if len(logSender.LogBuffer)+len(msg) <= cap(logSender.LogBuffer) { // still fits in buffer
			logSender.LogBuffer = append(logSender.LogBuffer, msg...)
			addedBefore = true
		}
		// otherwise send logs and reset buffer
		err = logSender.sendLogs()
		if err != nil {
			if !bufferLengthCondition { // if we fail and no space, drop it.
				err = fmt.Errorf("droped buffer, as sending failed and buffer was full: %v", err)
				logSender.LogBuffer = make([]byte, 0, cap(logSender.LogBuffer))
				logSender.LastSendTime = time.Now()
			} else {
				// Otherwise, we will try to send again in few seconds (10% of interval
				logSender.LastSendTime = time.Now().Add(-logSender.Interval / 10)
			}
		}
		if !addedBefore {
			// write unsent log to buffer
			logSender.LogBuffer = append(logSender.LogBuffer, msg...)
		}
	}
	return struct {
		bufferLengthCondition bool
		timeCondition         bool
		Err                   error
	}{bufferLengthCondition, timeCondition, err}
}

// This function should be called during shutdown
func (logSender *LogSender) FlushLogs() error {
	return logSender.sendLogs()
}

func (logSender *LogSender) sendLogs() error {
	if len(logSender.LogBuffer) == 0 {
		return nil
	}
	req, err := http.NewRequest("POST", logSender.Url.String(), bytes.NewReader(logSender.LogBuffer))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set(telemetry_headers.XTelemetryRemoteLog, "true") // value is arbitrary, just have to be non-empty
	req.Header.Set(telemetry_headers.ClientId, logSender.ClientId)
	resp, err := logSender.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("unexpected HTTP status code: " + strconv.Itoa(resp.StatusCode))
	}
	logSender.LogBuffer = make([]byte, 0, cap(logSender.LogBuffer))
	logSender.LastSendTime = time.Now()
	return nil
}
