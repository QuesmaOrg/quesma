// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package licensing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"quesma/logger"
	"time"
)

const (
	obtainLicenseEndpoint = "https://quesma-licensing-service-gd46dsvxda-uc.a.run.app/api/license/obtain"
	verifyLicenseEndpoint = "https://quesma-licensing-service-gd46dsvxda-uc.a.run.app/api/license/verify"
)

type InstallationIDPayload struct {
	InstallationID string `json:"installation_id"`
}

type LicensePayload struct {
	LicenseKey []byte `json:"license_key"`
}

// AllowList is returned by the license server based on the provided license key
type AllowList struct {
	InstallationID string    `json:"installation_id"`
	ClientName     string    `json:"client"`
	Connectors     []string  `json:"connectors"`
	Processors     []string  `json:"processors"`
	ExpirationDate time.Time `json:"expiration_date"`
}

func (a *AllowList) ToString() string {
	return fmt.Sprintf("[Quesma License]\n\tInstallation ID: %s\n\tClient Name: %s\n\tConnectors: %v\n\tProcessors: %v\n\tExpires: %s",
		a.InstallationID, a.ClientName, a.Connectors, a.Processors, a.ExpirationDate)
}

// obtainLicenseKey presents an InstallationId to the license server and receives a LicenseKey in return
func (l *LicenseModule) obtainLicenseKey() (err error) {
	logger.Info().Msgf("Obtaining license key for installation ID [%s]", l.InstallationID)
	var payloadBytes []byte
	if payloadBytes, err = json.Marshal(InstallationIDPayload{InstallationID: l.InstallationID}); err != nil {
		return err
	}
	resp, err := http.Post(obtainLicenseEndpoint, "application/json", bytes.NewReader(payloadBytes))
	defer resp.Body.Close()
	if err != nil {
		return err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var licenseResponse LicensePayload
	if err = json.Unmarshal(body, &licenseResponse); err != nil {
		return err
	}
	l.LicenseKey = licenseResponse.LicenseKey
	logger.Info().Msgf("License key obtained and set successfully, key=[%s]", l.LicenseKey)
	return nil
}

// processLicense presents the license to the license server and receives an AllowList in return
func (l *LicenseModule) processLicense() error {
	if allowList, err := l.fetchAllowList(); err != nil {
		return fmt.Errorf("failed processing license by the license server: %v", err)
	} else {
		l.AllowList = allowList
		logger.Info().Msgf("Allowlist loaded successfully\n%s", allowList.ToString())
	}
	if l.AllowList.ExpirationDate.Before(time.Now()) {
		return fmt.Errorf("license expired on %s", l.AllowList.ExpirationDate)
	}
	return nil
}

func (l *LicenseModule) fetchAllowList() (a *AllowList, err error) {
	logger.Info().Msgf("Presenting the license key to the license server for validation")
	var payloadBytes []byte
	if payloadBytes, err = json.Marshal(LicensePayload{LicenseKey: l.LicenseKey}); err != nil {
		return nil, err
	}
	resp, err := http.Post(verifyLicenseEndpoint, "application/json", bytes.NewReader(payloadBytes))
	defer resp.Body.Close()
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(body, &a); err != nil {
		return nil, err
	} else {
		return a, nil
	}
}
