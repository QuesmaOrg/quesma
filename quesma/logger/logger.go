// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package logger

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"io"
	"net/http"
	"os"
	"quesma/stats/errorstats"
	"quesma/tracing"
	"time"
)

const (
	stdLogFileName = "quesma.log"
	errLogFileName = "err.log"
)

var (
	StdLogFile *os.File
	ErrLogFile *os.File
)

const (
	RID                              = "request_id" // request id key for the logger
	Reason                           = "reason"     // Known error reason key for the logger
	Path                             = "path"
	AsyncId                          = "async_id"
	OpaqueId                         = "opaque_id"
	ReasonPrefixUnsupportedQueryType = "unsupported_search_query: " // Reason for Error messages for unsupported queries will start with this prefix
)

const (
	initialBufferSize = 32 * 1024
	bufferSizeChannel = 1024
)

var logger zerolog.Logger

// InitLogger returns channel where log messages will be sent
func InitLogger(cfg Configuration, sig chan os.Signal, doneCh chan struct{}, asyncQueryTraceLogger *tracing.AsyncTraceLogger) <-chan LogWithLevel {
	zerolog.TimeFieldFormat = time.RFC3339Nano // without this we don't have milliseconds timestamp precision
	var output io.Writer = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.StampMilli}
	if os.Getenv("GO_ENV") == "production" { // ConsoleWriter is slow, disable it in production
		output = os.Stderr
	}
	logChannel := make(chan LogWithLevel, 50000) // small number like 5 or 10 made entire Quesma totally unresponsive during the few seconds where Kibana spams with messages
	chanWriter := &channelWriter{ch: logChannel}

	var logWriters []io.Writer
	if cfg.FileLogging {
		openLogFiles(cfg.Path)
		logWriters = []io.Writer{output, StdLogFile, errorFileLogger{ErrLogFile}, chanWriter}
	} else {
		logWriters = []io.Writer{output, chanWriter}
	}
	if cfg.RemoteLogDrainUrl == nil {
		// FIXME
		// LogForwarder has extra jobs either. It forwards information that we're done.
		// This should be done  via context cancellation.
		go func() {
			<-sig
			doneCh <- struct{}{}
		}()
	} else {
		logDrainUrl := *cfg.RemoteLogDrainUrl
		logForwarder := LogForwarder{logSender: LogSender{
			Url:          &logDrainUrl,
			ClientId:     cfg.ClientId,
			LogBuffer:    make([]byte, 0, initialBufferSize),
			LastSendTime: time.Now(),
			Interval:     time.Minute,
			httpClient: &http.Client{
				Timeout: time.Minute,
			},
		}, logCh: make(chan []byte, bufferSizeChannel),
			ticker: time.NewTicker(time.Second),
			sigCh:  sig,
			doneCh: doneCh,
		}

		logForwarder.Run()
		logForwarder.TriggerFlush()
		logWriters = append(logWriters, &logForwarder)
	}

	multi := zerolog.MultiLevelWriter(logWriters...)
	logger = zerolog.New(multi).
		Level(cfg.Level).
		With().
		Timestamp().
		Caller().
		Logger()

	globalError := errorstats.GlobalErrorHook{}
	logger = logger.Hook(&globalError)
	if asyncQueryTraceLogger != nil {
		logger = logger.Hook(asyncQueryTraceLogger)
	}

	logger.Info().Msgf("Logger initialized with level %s", cfg.Level)
	return logChannel
}

// InitSimpleLoggerForTests initializes our global logger to the console output.
// Useful e.g. in debugging failing tests: you can call this function at the beginning
// of the test, and calls to the global logger will start appearing in the console.
// Without it, they don't.
func InitSimpleLoggerForTests() {
	logger = zerolog.New(
		zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.StampMilli,
		}).
		Level(zerolog.DebugLevel).
		With().
		Timestamp().
		Logger()
}

// InitSimpleLoggerForTestsWarnLevel initializes our global logger (level >= Warn) to the console output.
// Useful e.g. in debugging failing tests: you can call this function at the beginning
// of the test, and calls to the global logger will start appearing in the console.
// Without it, they don't.
func InitSimpleLoggerForTestsWarnLevel() {
	logger = zerolog.New(
		zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.StampMilli,
		}).
		Level(zerolog.WarnLevel).
		With().
		Timestamp().
		Logger()
}

var testChanWriter *channelWriter

func InitOnlyChannelLoggerForTests() <-chan LogWithLevel {

	// We can't reassign global logger, it will lead to "race condition" in tests. It's known issue with zerolog.
	// https://github.com/rs/zerolog/issues/242

	// So we replace the channel instead.

	// Our tests rely on a global logger. If we run them in parallel, they will interfere with each other.
	// So we don't care about locking here.

	if testChanWriter != nil {
		close(testChanWriter.ch)

		logChannel := make(chan LogWithLevel, 50000)
		testChanWriter.ch = logChannel
		return logChannel
	}

	zerolog.TimeFieldFormat = time.RFC3339Nano   // without this we don't have milliseconds timestamp precision
	logChannel := make(chan LogWithLevel, 50000) // small number like 5 or 10 made entire Quesma totally unresponsive during the few seconds where Kibana spams with messages
	chanWriter := &channelWriter{ch: logChannel}
	testChanWriter = chanWriter

	logger = zerolog.New(chanWriter).
		Level(zerolog.DebugLevel).
		With().
		Timestamp().
		Caller().
		Logger()

	globalError := errorstats.GlobalErrorHook{}
	logger = logger.Hook(&globalError)
	return logChannel
}

func openLogFiles(logsPath string) {
	var err error
	StdLogFile, err = os.OpenFile(
		fmt.Sprintf("%s/%s", logsPath, stdLogFileName),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0664,
	)
	if err != nil {
		panic(err)
	}
	ErrLogFile, err = os.OpenFile(
		fmt.Sprintf("%s/%s", logsPath, errLogFileName),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0664,
	)
	if err != nil {
		panic(err)
	}
}

func addKnownContextValues(event *zerolog.Event, ctx context.Context) *zerolog.Event {

	if requestId, ok := ctx.Value(tracing.RequestIdCtxKey).(string); ok {
		event = event.Str(RID, requestId)
	}
	if path, ok := ctx.Value(tracing.RequestPath).(string); ok {
		event = event.Str(Path, path)
	}
	if reason, ok := ctx.Value(tracing.ReasonCtxKey).(string); ok {
		event = event.Str(Reason, reason)
	}
	if asyncId, ok := ctx.Value(tracing.AsyncIdCtxKey).(string); ok {
		if asyncId != "" {
			event = event.Str(AsyncId, asyncId)
		}
	}

	if requestId, ok := ctx.Value(tracing.OpaqueIdCtxKey).(string); ok {
		event = event.Str(OpaqueId, requestId)
	}

	return event
}

func Debug() *zerolog.Event {
	return logger.Debug()
}

func DebugWithCtx(ctx context.Context) *zerolog.Event {
	event := logger.Debug().Ctx(ctx)
	event = addKnownContextValues(event, ctx)
	return event
}

func Info() *zerolog.Event {
	return logger.Info()
}

func InfoWithCtx(ctx context.Context) *zerolog.Event {
	event := logger.Info().Ctx(ctx)
	event = addKnownContextValues(event, ctx)
	return event
}

// MarkTraceEndWithCtx marks the end of a trace with the given context.
// Calling this functions at end of a trace is crucial from the transactional logging perspective.
func MarkTraceEndWithCtx(ctx context.Context) *zerolog.Event {
	event := logger.Info().Ctx(ctx)
	event = addKnownContextValues(event, ctx)
	ctx = context.WithValue(ctx, tracing.TraceEndCtxKey, true)
	event = event.Ctx(ctx)
	return event
}

func Warn() *zerolog.Event {
	return logger.Warn()
}

func WarnWithCtx(ctx context.Context) *zerolog.Event {
	event := logger.Warn().Ctx(ctx)
	event = addKnownContextValues(event, ctx)
	return event
}

func WarnWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event {
	return WarnWithCtx(context.WithValue(ctx, tracing.ReasonCtxKey, reason))
}

func Error() *zerolog.Event {
	return logger.Error()
}

func ErrorWithCtx(ctx context.Context) *zerolog.Event {
	event := logger.Error().Ctx(ctx)
	event = addKnownContextValues(event, ctx)
	return event
}

func ErrorWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event {
	return ErrorWithCtx(context.WithValue(ctx, tracing.ReasonCtxKey, reason))
}

func Fatal() *zerolog.Event {
	return logger.Fatal()
}

func Panic() *zerolog.Event {
	return logger.Panic()
}

func ReasonUnsupportedQuery(queryType string) string {
	return ReasonPrefixUnsupportedQueryType + queryType
}
