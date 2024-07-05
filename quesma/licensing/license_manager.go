// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package licensing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// obtainLicenseKey presents an InstallationId to the license server and receives a LicenseKey in return
func (l *LicenseModule) obtainLicenseKey() (err error) {
	l.logDebug("Obtaining license key for installation ID [%s]", l.InstallationID)
	var payloadBytes []byte
	if payloadBytes, err = json.Marshal(InstallationIDPayload{InstallationID: l.InstallationID}); err != nil {
		return err
	}
	resp, err := http.Post(obtainLicenseEndpoint, "application/json", bytes.NewReader(payloadBytes))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var licenseResponse LicensePayload
	if err = json.Unmarshal(body, &licenseResponse); err != nil {
		return err
	}
	l.LicenseKey = licenseResponse.LicenseKey
	fmt.Printf("License key obtained and set successfully, key=[%s.....%s]\n", string(l.LicenseKey[:8]), string(l.LicenseKey[len(l.LicenseKey)-8:]))
	return nil
}

// processLicense presents the license to the license server and receives an AllowList in return
func (l *LicenseModule) processLicense() error {
	if fetchedLicense, err := l.fetchLicense(); err != nil {
		return fmt.Errorf("license validation failed with: %v", err)
	} else {
		l.License = fetchedLicense
		l.logDebug("Allowlist loaded successfully")
		l.logDebug("%s", fetchedLicense.String())
	}
	if l.License.ExpirationDate.Before(time.Now()) {
		return fmt.Errorf("license expired on %s", l.License.ExpirationDate)
	}
	return nil
}

func (l *LicenseModule) fetchLicense() (a *License, err error) {
	var payloadBytes []byte
	if payloadBytes, err = json.Marshal(LicensePayload{LicenseKey: l.LicenseKey}); err != nil {
		return nil, err
	}
	resp, err := http.Post(verifyLicenseEndpoint, "application/json", bytes.NewReader(payloadBytes))
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("license key rejected by the License server")
	}
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
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
