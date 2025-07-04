// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import (
	"github.com/QuesmaOrg/quesma/platform/database_common"
)

type HydrolixConnector struct {
	Connector *database_common.LogManager
}

const hydrolixConnectorTypeName = "hydrolix"

func (h *HydrolixConnector) LicensingCheck() error {
	return h.Connector.CheckIfConnectedPaidService(database_common.CHCloudServiceName)
}

func (h *HydrolixConnector) Type() string {
	return hydrolixConnectorTypeName
}

func (h *HydrolixConnector) GetConnector() *database_common.LogManager {
	return h.Connector
}
