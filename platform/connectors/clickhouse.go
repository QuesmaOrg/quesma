// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import (
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
)

type ClickHouseConnector struct {
	Connector *clickhouse.LogManager
}

const clickHouseConnectorTypeName = "clickhouse"

func (c *ClickHouseConnector) LicensingCheck() (err error) {
	return c.Connector.CheckIfConnectedPaidService(clickhouse.HydrolixServiceName)
}

func (c *ClickHouseConnector) Type() string {
	return clickHouseConnectorTypeName
}

func (c *ClickHouseConnector) GetConnector() *clickhouse.LogManager {
	return c.Connector
}
