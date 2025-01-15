// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import (
	"github.com/QuesmaOrg/quesma/quesma/clickhouse"
)

type HydrolixConnector struct {
	Connector *clickhouse.LogManager
}

const hydrolixConnectorTypeName = "hydrolix"

func (h *HydrolixConnector) LicensingCheck() error {
	return h.Connector.CheckIfConnectedPaidService(clickhouse.CHCloudServiceName)
}

func (h *HydrolixConnector) Type() string {
	return hydrolixConnectorTypeName
}

func (h *HydrolixConnector) GetConnector() *clickhouse.LogManager {
	return h.Connector
}
