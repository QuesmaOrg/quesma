// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import "fmt"

type Checker interface {
	CheckHealth() Status
}

type Status struct {
	Status  string
	Message string
	Tooltip string
}

func NewStatus(status, message, tooltip string) Status {
	return Status{
		Status:  status,
		Message: message,
		Tooltip: tooltip,
	}
}

func (s Status) String() string {
	return fmt.Sprintf("%s: %s", s.Status, s.Message)
}
