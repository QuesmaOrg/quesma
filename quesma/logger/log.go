package logger

import "github.com/rs/zerolog"

type LogWithLevel struct {
	Level zerolog.Level
	Msg   string
}
