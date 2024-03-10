package logger

import (
	"bytes"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type LogSender struct {
	Url          *url.URL
	LogBuffer    []byte
	LastSendTime time.Time
	Interval     time.Duration
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
		// otherwise send logs and reset buffer
		err = logSender.sendLogs()
		if err == nil {
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
	if len(logSender.LogBuffer) == 0 { // I would move it to sendLogs(), but it breaks tests
		return nil
	}
	return logSender.sendLogs()
}

func (logSender *LogSender) sendLogs() error {
	resp, err := http.Post(logSender.Url.String(), "text/plain", bytes.NewReader(logSender.LogBuffer))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("unexpected HTTP status code: " + strconv.Itoa(resp.StatusCode))
	}
	logSender.LogBuffer = make([]byte, 0, cap(logSender.LogBuffer))
	logSender.LastSendTime = time.Now()
	return nil
}
