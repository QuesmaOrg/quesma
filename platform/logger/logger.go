// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package logger

import (
	"context"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/stats/errorstats"
	quesma_v2 "github.com/QuesmaOrg/quesma/platform/v2/core"
	"github.com/QuesmaOrg/quesma/platform/v2/core/tracing"
	"github.com/rs/zerolog"
	"io"
	"net/http"
	"os"

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

// InitLogger returns channel where log messages will be sent
func InitLogger(cfg Configuration, sig chan os.Signal, doneCh chan struct{}) <-chan LogWithLevel {
	zerolog.TimeFieldFormat = time.RFC3339Nano // without this we don't have milliseconds timestamp precision
	var output io.Writer = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.StampMilli}
	if os.Getenv("GO_ENV") == "production" { // ConsoleWriter is slow, disable it in production
		output = os.Stderr
	}
	logChannel := make(chan LogWithLevel, 50000) // small number like 5 or 10 made entire Quesma totally unresponsive during the few seconds where Kibana spams with messages
	chanWriter := channelWriter{ch: logChannel}

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
	l := zerolog.New(multi).
		Level(cfg.Level).
		Sample(&zerolog.BurstSampler{
			Burst:       quesma_v2.DefaultBurstSamplerMaxLogsPerSecond * quesma_v2.DefaultBurstSamplerPeriodSeconds,
			Period:      quesma_v2.DefaultBurstSamplerPeriodSeconds * time.Second,
			NextSampler: zerolog.RandomSampler(quesma_v2.DefaultSheddingFrequency),
		}).
		With().
		Timestamp().
		Caller().
		Logger()

	globalError := errorstats.GlobalErrorHook{}
	l = l.Hook(&globalError)

	l.Info().Msgf("Logger initialized with level %s", cfg.Level)

	logger = quesma_v2.NewQuesmaLogger(l)

	return logChannel
}

// InitSimpleLoggerForTests initializes our global logger to the console output.
// Useful e.g. in debugging failing tests: you can call this function at the beginning
// of the test, and calls to the global logger will start appearing in the console.
// Without it, they don't.
func InitSimpleLoggerForTests() {
	l := zerolog.New(
		zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.StampMilli,
		}).
		Level(zerolog.DebugLevel).
		With().
		Timestamp().
		Logger()

	logger = quesma_v2.NewQuesmaLogger(l)
}

// InitSimpleLoggerForTestsWarnLevel initializes our global logger (level >= Warn) to the console output.
// Useful e.g. in debugging failing tests: you can call this function at the beginning
// of the test, and calls to the global logger will start appearing in the console.
// Without it, they don't.
func InitSimpleLoggerForTestsWarnLevel() {
	l := zerolog.New(
		zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.StampMilli,
		}).
		Level(zerolog.WarnLevel).
		With().
		Timestamp().
		Logger()

	logger = quesma_v2.NewQuesmaLogger(l)
}

var testLoggerInitialized bool

const TestConsoleStatsBasedOnLogs = false

func InitOnlyChannelLoggerForTests() <-chan LogWithLevel {

	// We can't reassign global logger, it will lead to "race condition" in tests. It's known issue with zerolog.
	// https://github.com/rs/zerolog/issues/242

	if testLoggerInitialized {
		// we do return a fresh channel here, it will break the stats gathering in the console
		// see TestConsoleStatsBasedOnLogs usage in the tests
		return make(chan LogWithLevel, 50000)
	}

	zerolog.TimeFieldFormat = time.RFC3339Nano   // without this we don't have milliseconds timestamp precision
	logChannel := make(chan LogWithLevel, 50000) // small number like 5 or 10 made entire Quesma totally unresponsive during the few seconds where Kibana spams with messages
	chanWriter := channelWriter{ch: logChannel}

	l := zerolog.New(chanWriter).
		Level(zerolog.DebugLevel).
		With().
		Timestamp().
		Caller().
		Logger()

	globalError := errorstats.GlobalErrorHook{}
	l = l.Hook(&globalError)

	logger = quesma_v2.NewQuesmaLogger(l)

	testLoggerInitialized = true
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

// --- legacy API

// global logger, TODO  this should be removed
var logger = quesma_v2.EmptyQuesmaLogger()

func GlobalLogger() quesma_v2.QuesmaLogger {
	return logger
}

// global logger delegates

func Debug() *zerolog.Event {
	return logger.Debug()
}

func DebugWithCtx(ctx context.Context) *zerolog.Event {
	return logger.DebugWithCtx(ctx)
}

func DebugWithReason(reason string) *zerolog.Event {
	return logger.DebugWithReason(reason)
}

func DebugWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event {
	return logger.DebugWithCtxAndReason(ctx, reason)
}

func DebugFull(ctx context.Context, reason string, err error) *zerolog.Event {
	ctx = context.WithValue(ctx, tracing.ErrorCtxKey, err)
	return logger.DebugWithCtxAndReason(ctx, reason)
}

func Info() *zerolog.Event {
	return logger.Info()
}

func InfoWithCtx(ctx context.Context) *zerolog.Event {
	return logger.InfoWithCtx(ctx)
}

func InfoWithReason(reason string) *zerolog.Event {
	return logger.InfoWithReason(reason)
}

func InfoWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event {
	return logger.InfoWithCtxAndReason(ctx, reason)
}

func InfoFull(ctx context.Context, reason string, err error) *zerolog.Event {
	ctx = context.WithValue(ctx, tracing.ErrorCtxKey, err)
	return logger.InfoWithCtxAndReason(ctx, reason)
}

// MarkTraceEndWithCtx marks the end of a trace with the given context.
// Calling this functions at end of a trace is crucial from the transactional logging perspective.
func MarkTraceEndWithCtx(ctx context.Context) *zerolog.Event {
	return logger.MarkTraceEndWithCtx(ctx)
}

func Warn() *zerolog.Event {
	return logger.Warn()
}

func WarnWithCtx(ctx context.Context) *zerolog.Event {
	return logger.WarnWithCtx(ctx)
}

func WarnWithReason(reason string) *zerolog.Event {
	return logger.WarnWithReason(reason)
}

func WarnWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event {
	return logger.WarnWithCtxAndReason(ctx, reason)
}

func WarnFull(ctx context.Context, reason string, err error) *zerolog.Event {
	ctx = context.WithValue(ctx, tracing.ErrorCtxKey, err)
	return logger.WarnWithCtxAndReason(ctx, reason)
}

func Error() *zerolog.Event {
	return logger.Error()
}

func ErrorWithCtx(ctx context.Context) *zerolog.Event {
	return logger.ErrorWithCtx(ctx)
}

func ErrorWithReason(reason string) *zerolog.Event {
	return logger.ErrorWithReason(reason)
}

func ErrorWithCtxAndReason(ctx context.Context, reason string) *zerolog.Event {
	return logger.ErrorWithCtxAndReason(ctx, reason)
}

func ErrorFull(ctx context.Context, reason string, err error) *zerolog.Event {
	ctx = context.WithValue(ctx, tracing.ErrorCtxKey, err)
	return logger.ErrorWithCtxAndReason(ctx, reason)
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

func DeduplicatedInfo() quesma_v2.DeduplicatedEvent {
	return logger.DeduplicatedInfo()
}

func DeduplicatedWarn() quesma_v2.DeduplicatedEvent {
	return logger.DeduplicatedWarn()
}
