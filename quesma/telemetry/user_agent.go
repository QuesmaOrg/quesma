// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package telemetry

import (
	"regexp"
)

func processUserAgent(userAgent string) string {

	matchVersion := regexp.MustCompile(`(Chrome|Mozilla|Gecko|Firefox|Trident|Safari|Ubuntu|AppleWebKit|Edge|Version)/[0-9\\.]+`)

	userAgent = matchVersion.ReplaceAllString(userAgent, "$1/*")

	return userAgent
}
