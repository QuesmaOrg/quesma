// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/util"
	"net/http"
	"net/url"
	"testing"
)

func TestIsWriteRequest(t *testing.T) {
	tests := []struct {
		method string
		url    string
		want   bool
	}{
		{method: http.MethodPost, url: "/_bulk", want: true},
		{method: http.MethodPost, url: "/_doc", want: true},
		{method: http.MethodPost, url: "/_create", want: true},
		{method: http.MethodPut, url: "/_create", want: true},
		{method: http.MethodPost, url: "/_search", want: false},
	}
	for i, tt := range tests {
		t.Run(util.PrettyTestName(fmt.Sprintf("%s %s", tt.method, tt.url), i), func(t *testing.T) {
			if got := IsWriteRequest(&http.Request{
				Method: tt.method,
				URL:    &url.URL{Path: tt.url},
			}); got != tt.want {
				t.Errorf("IsWriteRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}
