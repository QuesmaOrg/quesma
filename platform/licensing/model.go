// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package licensing

import (
	"fmt"
	"strings"
	"time"
)

// License is an object returned by the license server based on the provided (and positively verified) license key
type License struct {
	InstallationID string    `json:"installation_id"`
	ClientID       string    `json:"client_id"`
	Connectors     []string  `json:"connectors"`
	Processors     []string  `json:"processors"`
	ExpirationDate time.Time `json:"expiration_date"`
}

func (a *License) String() string {
	return fmt.Sprintf("[Quesma License]\n\tInstallation ID: %s\n\tClient Name: %s\n\tConnectors: [%v]\n\tProcessors: [%v]\n\tExpires: %s\n",
		a.InstallationID, a.ClientID, strings.Join(a.Connectors, ", "), strings.Join(a.Processors, ", "), a.ExpirationDate)
}
