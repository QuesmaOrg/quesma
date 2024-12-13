// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package diag

type Diagnostic interface {
	PhoneHomeAgent() PhoneHomeClient
	DebugInfoCollector() DebugInfoCollector
}

type diagnosticImpl struct {
	phoneHomeAgent     PhoneHomeClient
	debugInfoCollector DebugInfoCollector
}

func NewStatistics(phoneHomeAgent PhoneHomeClient, debugInfoCollector DebugInfoCollector) Diagnostic {
	return &diagnosticImpl{
		phoneHomeAgent:     phoneHomeAgent,
		debugInfoCollector: debugInfoCollector,
	}
}

func (s *diagnosticImpl) PhoneHomeAgent() PhoneHomeClient {
	return s.phoneHomeAgent
}

func (s *diagnosticImpl) DebugInfoCollector() DebugInfoCollector {
	return s.debugInfoCollector
}
