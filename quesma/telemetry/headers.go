// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package telemetry

// RemoteLogHeaderName informs the telemetry endpoint that the payload contains logs instead of classic phone home data.
const RemoteLogHeaderName = "X-Telemetry-Remote-Log"

// ClientIdHeaderName This header is used to identify the client in telemetry data.
// It has been introduced after creating Quesma licensing module and it is obtained from a validated license.
const ClientIdHeaderName = "X-Client-Id"
