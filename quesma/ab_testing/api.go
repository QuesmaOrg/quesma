// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ab_testing

import (
	"time"
)

type Request struct {
	Path string `json:"path"`
	Body string `json:"body"`
}

type Response struct {
	Body string        `json:"body"`
	Time time.Duration `json:"time"`
}

type Result struct {
	Request Request  `json:"request"`
	A       Response `json:"request_a"`
	B       Response `json:"request_b"`
	// add other fields if needed
}

type ResultsRepository interface {
	Store(result Result)
}
