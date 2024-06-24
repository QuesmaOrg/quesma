package connectors

import (
	"quesma/clickhouse"
	"quesma/logger"
)

type HydrolixConnector struct {
	Connector *clickhouse.LogManager
}

const hydrolixConnectorTypeName = "hydrolix"

func (h *HydrolixConnector) LicensingCheck() error {
	logger.Debug().Msg("Runtime checks for Hydrolix connector is not required, as static configuration disables it.")
	return nil
}

func (h *HydrolixConnector) Type() string {
	return hydrolixConnectorTypeName
}

func (h *HydrolixConnector) GetConnector() *clickhouse.LogManager {
	return h.Connector
}
