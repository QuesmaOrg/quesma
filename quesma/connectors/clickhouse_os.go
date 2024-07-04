// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"quesma/clickhouse"
	"quesma/logger"
	"quesma/quesma/config"
	"strings"
)

type ClickHouseOSConnector struct {
	Connector *clickhouse.LogManager
}

const clickHouseOSConnectorTypeName = "clickhouse-os"

func (c *ClickHouseOSConnector) LicensingCheck() error {
	dbUrlConfigured := c.Connector.GetDBUrl()
	if dbUrlConfigured == nil {
		return errors.New("database URL for ClickHouseOS connector is not configured")
	}
	checksCount := 3
	errChan := make(chan error, checksCount)
	go func() { errChan <- c.checkIfCloudHostnameConfigured(dbUrlConfigured) }()
	go func() { errChan <- c.checkIfCloudIPWillBeUsed(dbUrlConfigured) }()
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

func (c *ClickHouseOSConnector) checkIfCloudHostnameConfigured(dbUrlConfigured *config.Url) error {
	var clickhouseCloudDomains = []string{"clickhouse.cloud", "clickhouse.com"}
	for _, domain := range clickhouseCloudDomains {
		if strings.Contains(dbUrlConfigured.String(), domain) {
			return errors.New(clickhouseCloudConnectError)
		}
	}
	return nil
}

func (c *ClickHouseOSConnector) checkIfCloudIPWillBeUsed(dbUrlConfigured *config.Url) (err error) {
	var configuredIPs, chCloudIPs []string
	if chCloudIPs, err = fetchClickHouseCloudIpAddresses(); err != nil {
		return err
	}
	if ips, err := net.LookupIP(dbUrlConfigured.Hostname()); err != nil {
		logger.Debug().Msgf("Error when looking up IPs for %s: %v", dbUrlConfigured.Hostname(), err)
	} else {
		for _, ip := range ips {
			configuredIPs = append(configuredIPs, ip.String())
		}
	}
	logger.Info().Msgf("Configured IPs=[%s] CH Cloud IPs=[%s]", strings.Join(configuredIPs, ","), strings.Join(chCloudIPs, ","))
	if containsAny(configuredIPs, chCloudIPs) {
		return errors.New(clickhouseCloudConnectError)
	}
	return nil
}

func fetchClickHouseCloudIpAddresses() ([]string, error) {
	resp, err := http.Get("https://api.clickhouse.cloud/static-ips.json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var response Response
	if err = json.Unmarshal(body, &response); err != nil {
		return nil, err
	}

	var ips []string
	for _, region := range response.Aws {
		ips = append(ips, region.EgressIps...)
		ips = append(ips, region.IngressIps...)
	}
	for _, region := range response.Azure {
		ips = append(ips, region.EgressIps...)
		ips = append(ips, region.IngressIps...)
	}
	for _, region := range response.Gcp {
		ips = append(ips, region.EgressIps...)
		ips = append(ips, region.IngressIps...)
	}

	return ips, nil
}

type Response struct {
	Aws   []Region `json:"aws"`
	Azure []Region `json:"azure"`
	Gcp   []Region `json:"gcp"`
}

type Region struct {
	EgressIps  []string `json:"egress_ips"`
	IngressIps []string `json:"ingress_ips"`
	Region     string   `json:"region"`
}

// containsAny is a helper func which returns true if a contains any element of b.
func containsAny(a, b []string) bool {
	for _, v := range a {
		for _, w := range b {
			if v == w {
				return true
			}
		}
	}
	return false
}
