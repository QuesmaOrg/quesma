// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import (
	"github.com/QuesmaOrg/quesma/platform/database_common"
)

type ClickHouseOSConnector struct {
	Connector *database_common.LogManager
}

const clickHouseOSConnectorTypeName = "clickhouse-os"

func (c *ClickHouseOSConnector) LicensingCheck() error {
	checksCount := 2
	errChan := make(chan error, checksCount)
	go func() { errChan <- c.Connector.CheckIfConnectedPaidService(database_common.CHCloudServiceName) }()
	go func() { errChan <- c.Connector.CheckIfConnectedPaidService(database_common.HydrolixServiceName) }()
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

func (c *ClickHouseOSConnector) GetConnector() *database_common.LogManager {
	return c.Connector
}
