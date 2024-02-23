package logger

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"io"
	"mitmproxy/quesma/quesma/config"
	"mitmproxy/quesma/stats/errorstats"
	"mitmproxy/quesma/tracing"
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
	RID    = "request_id" // request id key for the logger
	Reason = "reason"     // Known error reason key for the logger
)

var logger zerolog.Logger

// Returns channel where log messages will be sent
func InitLogger(cfg config.QuesmaConfiguration) <-chan string {
	zerolog.TimeFieldFormat = time.RFC3339Nano // without this we don't have milliseconds timestamp precision
	var output io.Writer = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.StampMilli}
	if os.Getenv("GO_ENV") == "production" { // ConsoleWriter is slow, disable it in production
		output = os.Stderr
	}
	openLogFiles(cfg.LogsPath)

	logChannel := make(chan string, 50000) // small number like 5 or 10 made entire Quesma totally unresponsive during the few seconds where Kibana spams with messages
	chanWriter := channelWriter{ch: logChannel}
	multi := zerolog.MultiLevelWriter(output, StdLogFile, errorFileLogger{ErrLogFile}, chanWriter)
	logger = zerolog.New(multi).
		Level(cfg.LogLevel).
		With().
		Timestamp().
		Caller().
		Logger()

	globalError := errorstats.GlobalErrorHook{}
	logger = logger.Hook(&globalError)

	logger.Info().Msg("Logger initialized")
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

func Debug() *zerolog.Event {
	return logger.Debug()
}

func Info() *zerolog.Event {
	return logger.Info()
}

func Warn() *zerolog.Event {
	return logger.Warn()
}

func Error() *zerolog.Event {
	return logger.Error()
}

func ErrorWithCtx(ctx context.Context) *zerolog.Event {
	event := logger.Error().Ctx(ctx)
	if requestId, ok := ctx.Value(tracing.RequestIdCtxKey).(string); ok {
		event = event.Str(RID, requestId)
	}
	if reason, ok := ctx.Value(tracing.ReasonCtxKey).(string); ok {
		event = event.Str(Reason, reason)
	}
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
