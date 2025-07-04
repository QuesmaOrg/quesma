// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package connectors

import (
	"github.com/QuesmaOrg/quesma/platform/database_common"
)

type DorisConnector struct {
	Connector *database_common.LogManager
}

const dorisConnectorTypeName = "doris"

func (c *DorisConnector) LicensingCheck() (err error) {
	return
}

func (c *DorisConnector) Type() string {
	return dorisConnectorTypeName
}

func (c *DorisConnector) GetConnector() *database_common.LogManager {
	return c.Connector
}
