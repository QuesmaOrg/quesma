// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_ParseInterval(t *testing.T) {
	type args struct {
		fixedInterval string
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{name: "1minute", args: args{fixedInterval: "1m"}, want: 1 * time.Minute},
		{name: "5minutes", args: args{fixedInterval: "5m"}, want: 5 * time.Minute},
		{name: "15minutes", args: args{fixedInterval: "15m"}, want: 15 * time.Minute},
		{name: "1hour", args: args{fixedInterval: "1h"}, want: 1 * time.Hour},
		{name: "1day", args: args{fixedInterval: "1d"}, want: 24 * time.Hour},
		{name: "1week", args: args{fixedInterval: "1w"}, want: 7 * 24 * time.Hour},
		{name: "1month", args: args{fixedInterval: "1M"}, want: 30 * 24 * time.Hour},
		{name: "1year", args: args{fixedInterval: "1y"}, want: 365 * 24 * time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := ParseInterval(tt.args.fixedInterval)
			assert.Equalf(t, tt.want, got, "ParseInterval(%v)", tt.args.fixedInterval)
		})
	}
}
