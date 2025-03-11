// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package logger

import (
	"github.com/rs/zerolog"
	"net/url"
)

type Configuration struct {
	FileLogging       bool
	Path              string
	RemoteLogDrainUrl *url.URL
	Level             zerolog.Level
	ClientId          string
}
