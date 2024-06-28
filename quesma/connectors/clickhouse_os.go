// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import "quesma/clickhouse"

type ClickHouseOSConnector struct {
	Connector *clickhouse.LogManager
}

const clickHouseOSConnectorTypeName = "clickhouse-os"

func (c *ClickHouseOSConnector) LicensingCheck() error {
	// TODO: Check if you're connected to ClickHouse Cloud OR Hydrolix and fail if so
	return c.Connector.CheckIfConnectedToHydrolix()
}

func (c *ClickHouseOSConnector) Type() string {
	return clickHouseOSConnectorTypeName
}

func (c *ClickHouseOSConnector) GetConnector() *clickhouse.LogManager {
	return c.Connector
}
