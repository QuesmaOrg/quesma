// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package queryparser

import "github.com/goccy/go-json"

func BadRequestParseError(err error) []byte {
	serialized, _ := json.Marshal(DashboardErrorResponse{
		Error: Error{
			RootCause: []RootCause{
				{
					Type:   "parsing_exception",
					Reason: err.Error(),
				},
			},
			Type:   "parsing_exception",
			Reason: err.Error(),
		},
		Status: 400,
	},
	)
	return serialized
}

func InternalQuesmaError(msg string) []byte {
	serialized, _ := json.Marshal(DashboardErrorResponse{
		Error: Error{
			RootCause: []RootCause{
				{
					Type:   "quesma_error",
					Reason: msg,
				},
			},
			Type:   "quesma_error",
			Reason: msg,
		},
		Status: 500,
	},
	)
	return serialized
}

type (
	DashboardErrorResponse struct {
		Error  `json:"error"`
		Status int `json:"status"`
	}
	Error struct {
		RootCause []RootCause `json:"root_cause"`
		Type      string      `json:"type"`
		Reason    string      `json:"reason"`
		Line      *int        `json:"line,omitempty"`
		Col       *int        `json:"col,omitempty"`
	}
	RootCause struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
		Line   *int   `json:"line,omitempty"`
		Col    *int   `json:"col,omitempty"`
	}
)
