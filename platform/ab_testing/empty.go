// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package ab_testing

type emptySender struct{}

func (e *emptySender) Send(result Result) {
	// do nothing
}

func NewEmptySender() Sender {
	return &emptySender{}
}
