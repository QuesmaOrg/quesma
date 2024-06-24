package connectors

import (
	"quesma/clickhouse"
)

type ClickHouseConnector struct {
	Connector *clickhouse.LogManager
}

const clickHouseConnectorTypeName = "clickhouse"

func (c *ClickHouseConnector) LicensingCheck() (err error) {
	return c.Connector.CheckIfConnectedToHydrolix()
}

func (c *ClickHouseConnector) Type() string {
	return clickHouseConnectorTypeName
}

func (c *ClickHouseConnector) GetConnector() *clickhouse.LogManager {
	return c.Connector
}
