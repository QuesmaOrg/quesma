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
