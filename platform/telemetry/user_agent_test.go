// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package telemetry

import "testing"

func TestProcessUserAgent(t *testing.T) {

	// https://www.useragentstring.com/pages/Browserlist/

	tests := []struct {
		given    string
		expected string
	}{
		{"Kibana/1.0", "Kibana/1.0"},
		{"Chrome/123", "Chrome/*"},
		{"Go-http-client/1.1", "Go-http-client/1.1"},
		{"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",

			"Mozilla/* (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/* (KHTML, like Gecko) Chrome/* Safari/*"},
	}

	for _, tt := range tests {
		t.Run(tt.given, func(t *testing.T) {
			actual := processUserAgent(tt.given)
			if actual != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, actual)
			}
		})
	}
}
