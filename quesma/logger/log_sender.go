package logger

import (
	"bytes"
	"errors"
	"fmt"
	"mitmproxy/quesma/quesma/config"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type LogSender struct {
	Url          *url.URL
	LicenseKey   string
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
		if err != nil && !bufferLengthCondition { // if we fail, but got space, we will retry later. Otherwise drop.
			err = fmt.Errorf("droped buffer, as sending failed and buffer was full: %v", err)
			logSender.LogBuffer = make([]byte, 0, cap(logSender.LogBuffer))
			logSender.LastSendTime = time.Now()
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
	req.Header.Set(config.RemoteLogHeader, "true") // value is arbitrary, just have to be non-empty
	req.Header.Set(config.LicenseHeader, logSender.LicenseKey)
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
