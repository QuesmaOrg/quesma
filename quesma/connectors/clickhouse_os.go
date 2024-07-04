// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import (
	"errors"
	"fmt"
	"quesma/clickhouse"
	"strings"
)

type ClickHouseOSConnector struct {
	Connector *clickhouse.LogManager
}

const clickHouseOSConnectorTypeName = "clickhouse-os"

func (c *ClickHouseOSConnector) LicensingCheck() error {
	checksCount := 2
	errChan := make(chan error, checksCount)
	go func() { errChan <- c.checkIfCloudHostnameConfigured() }()
	go func() { errChan <- c.Connector.CheckIfConnectedToHydrolix() }()
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

var clickhouseCloudConnectError = fmt.Sprintf("%s connector is not allowed to connect to ClickHouse Cloud installations", clickHouseOSConnectorTypeName)

func (c *ClickHouseOSConnector) checkIfCloudHostnameConfigured() error {
	dbUrlConfigured := c.Connector.GetDBUrl()
	var clickhouseCloudDomains = []string{"clickhouse.cloud", "clickhouse.com"}
	for _, domain := range clickhouseCloudDomains {
		if strings.Contains(dbUrlConfigured, domain) {
			return errors.New(clickhouseCloudConnectError)
		}
	}
	return nil
}
