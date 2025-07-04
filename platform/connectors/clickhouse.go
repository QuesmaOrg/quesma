// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import (
	"github.com/QuesmaOrg/quesma/platform/database_common"
)

type ClickHouseConnector struct {
	Connector *database_common.LogManager
}

const clickHouseConnectorTypeName = "clickhouse"

func (c *ClickHouseConnector) LicensingCheck() (err error) {
	return c.Connector.CheckIfConnectedPaidService(database_common.HydrolixServiceName)
}

func (c *ClickHouseConnector) Type() string {
	return clickHouseConnectorTypeName
}

func (c *ClickHouseConnector) GetConnector() *database_common.LogManager {
	return c.Connector
}
