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
	LicenseKey        string
	RemoteLogHeader   string
	LicenseHeader     string
}
