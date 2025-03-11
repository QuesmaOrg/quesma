// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package logger

import "github.com/rs/zerolog"

type LogWithLevel struct {
	Level zerolog.Level
	Msg   string
}
