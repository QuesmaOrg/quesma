package config

import "sync/atomic"

var TrafficAnalysis atomic.Bool

func SetTrafficAnalysis(val bool) {
	TrafficAnalysis.Store(val)
}
