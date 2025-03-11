// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package elasticsearch

import (
	"net/http"
	"strings"
)

func IsWriteRequest(req *http.Request) bool {
	// Elastic API is not regular, and it is hard to determine if the request is read or write.
	// We would like to keep this separate from the router configuration.
	switch req.Method {
	case http.MethodPost:
		if strings.Contains(req.URL.Path, "/_bulk") ||
			strings.Contains(req.URL.Path, "/_doc") ||
			strings.Contains(req.URL.Path, "/_create") {
			return true
		}
		// other are read
	case http.MethodPut, http.MethodDelete:
		return true
	}
	return false
}

func AddBasicAuthIfNeeded(req *http.Request, user, password string) *http.Request {
	if user != "" && password != "" {
		req.SetBasicAuth(user, password)
	}
	return req
}
