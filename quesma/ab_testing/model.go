// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ab_testing

const ABTestingTableName = "ab_testing_logs"

type Request struct {
	Path      string `json:"path"`
	IndexName string `json:"index_name"`
	Body      string `json:"body"`
}

type Response struct {
	Name  string  `json:"name"`
	Body  string  `json:"body"`
	Time  float64 `json:"time"`
	Error string  `json:"error"`
}

type Result struct {
	RequestID string `json:"request_id"`
	OpaqueID  string `json:"opaque_id"`

	Request Request  `json:"request"`
	A       Response `json:"response_a"`
	B       Response `json:"response_b"`
}

// Sender sends results to a destination. This one will be used in Quesma core.
type Sender interface {
	Send(result Result)
}

type HealthMessage struct {
	IsHealthy bool
}
