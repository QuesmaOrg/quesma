// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import (
	"quesma/clickhouse"
	"quesma/logger"
)

type HydrolixConnector struct {
	Connector *clickhouse.LogManager
}

const hydrolixConnectorTypeName = "hydrolix"

func (h *HydrolixConnector) LicensingCheck() error {
	logger.Debug().Msg("Runtime checks for Hydrolix connector is not required, as static configuration disables it.")
	return nil
}

func (h *HydrolixConnector) Type() string {
	return hydrolixConnectorTypeName
}

func (h *HydrolixConnector) GetConnector() *clickhouse.LogManager {
	return h.Connector
}
