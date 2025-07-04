// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import (
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/database_common"
	"github.com/QuesmaOrg/quesma/platform/licensing"
	"github.com/QuesmaOrg/quesma/platform/logger"
	"github.com/QuesmaOrg/quesma/platform/telemetry"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
)

type Connector interface {
	LicensingCheck() error
	Type() string
	GetConnector() *database_common.LogManager // enforce contract for having connector instance ... maybe unnecessary
}

type ConnectorManager struct {
	connectors []Connector
}

// GetConnector - TODO this is just bypassing the fact that we support only 1 connector at a time today :>
func (c *ConnectorManager) GetConnector() *database_common.LogManager {
	if len(c.connectors) == 0 {
		panic("No connectors found")
	}
	conn := c.connectors[0]
	if !c.connectors[0].GetConnector().IsInTransparentProxyMode() {
		go func() {
			if err := conn.LicensingCheck(); err != nil {
				licensing.PanicWithLicenseViolation(fmt.Errorf("connector [%s] reported licensing issue: [%v]", conn.Type(), err))
			}
		}()
	}
	return c.connectors[0].GetConnector()
}

func NewConnectorManager(cfg *config.QuesmaConfiguration, chDb quesma_api.BackendConnector, phoneHomeAgent telemetry.PhoneHomeAgent, loader database_common.TableDiscovery) *ConnectorManager {
	return &ConnectorManager{
		connectors: registerConnectors(cfg, chDb, phoneHomeAgent, loader),
	}
}

func registerConnectors(cfg *config.QuesmaConfiguration, chDb quesma_api.BackendConnector, phoneHomeAgent telemetry.PhoneHomeAgent, loader database_common.TableDiscovery) (conns []Connector) {
	for connName, conn := range cfg.Connectors {
		logger.Info().Msgf("Registering connector named [%s] of type [%s]", connName, conn.ConnectorType)
		switch conn.ConnectorType {
		case clickHouseConnectorTypeName:
			conns = append(conns, &ClickHouseConnector{
				Connector: database_common.NewEmptyLogManager(cfg, chDb, phoneHomeAgent, loader),
			})
		case clickHouseOSConnectorTypeName:
			conns = append(conns, &ClickHouseOSConnector{
				Connector: database_common.NewEmptyLogManager(cfg, chDb, phoneHomeAgent, loader),
			})
		case hydrolixConnectorTypeName:
			conns = append(conns, &HydrolixConnector{
				Connector: database_common.NewEmptyLogManager(cfg, chDb, phoneHomeAgent, loader),
			})
		case dorisConnectorTypeName:
			conns = append(conns, &DorisConnector{
				Connector: database_common.NewEmptyLogManager(cfg, chDb, phoneHomeAgent, loader),
			})
		default:
			logger.Error().Msgf("Unknown connector type [%s]", conn.ConnectorType)
		}
	}

	// Mock connector for transparent proxy, perhaps improve at some point
	if len(cfg.Connectors) == 0 && cfg.TransparentProxy {
		conns = append(conns, &ClickHouseOSConnector{
			Connector: database_common.NewEmptyLogManager(cfg, chDb, phoneHomeAgent, loader),
		})
	}

	return conns
}
