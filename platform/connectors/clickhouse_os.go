// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import (
	"github.com/QuesmaOrg/quesma/platform/clickhouse"
)

type ClickHouseOSConnector struct {
	Connector *clickhouse.LogManager
}

const clickHouseOSConnectorTypeName = "clickhouse-os"

func (c *ClickHouseOSConnector) LicensingCheck() error {
	checksCount := 2
	errChan := make(chan error, checksCount)
	go func() { errChan <- c.Connector.CheckIfConnectedPaidService(clickhouse.CHCloudServiceName) }()
	go func() { errChan <- c.Connector.CheckIfConnectedPaidService(clickhouse.HydrolixServiceName) }()
	for i := 0; i < checksCount; i++ {
		if err := <-errChan; err != nil {
			return err
		}
	}
	return nil
}

func (c *ClickHouseOSConnector) Type() string {
	return clickHouseOSConnectorTypeName
}

func (c *ClickHouseOSConnector) GetConnector() *clickhouse.LogManager {
	return c.Connector
}
