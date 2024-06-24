package licensing

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	licenseEndpoint = "https://license.quesma.com/v1/license"
)

type InstallationIDPayload struct {
	InstallationID string `json:"installation_id"`
}

type LicensePayload struct {
	LicenseKey []byte `json:"license_key"`
}

// AllowList is returned by the license server based on the provided license key
type AllowList struct {
	Connectors     []string  `json:"connectors"`
	Processors     []string  `json:"processors"`
	ExpirationDate time.Time `json:"expiration_date"`
}

// obtainLicenseKey presents an InstallationId to the license server and receives a LicenseKey in return
func (l *LicenseModule) obtainLicenseKey() (err error) {
	/* TODO we're just mocking this call for now (backend is not ready)
	var payloadBytes []byte
	if payloadBytes, err = json.Marshal(InstallationIDPayload{InstallationID: l.InstallationID}); err != nil {
		return err
	}
	resp, err := http.Post(licenseEndpoint, "application/json", bytes.NewReader(payloadBytes))
	defer resp.Body.Close()
	if err != nil {
		return err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var licenseResponse LicenseEndpointResponse
	if err = json.Unmarshal(body, &licenseResponse); err != nil {
		return err
	}
	l.LicenseKey = licenseResponse.LicenseKey
	*/
	time.Sleep(3 * time.Second)
	l.LicenseKey = []byte("PRZEMYSLAW-BLESSED-INSTANCE")
	//
	return nil
}

// processLicense presents the license to the license server and receives an AllowList in return
func (l *LicenseModule) processLicense() error {
	if allowList, err := l.fetchAllowList(); err != nil {
		return fmt.Errorf("failed processing license by the license server: %v", err)
	} else {
		l.AllowList = allowList
	}
	if l.AllowList.ExpirationDate.Before(time.Now()) && false { //TODO shadowing check for now
		return fmt.Errorf("license expired on %s", l.AllowList.ExpirationDate)
	}
	return nil
}

func (l *LicenseModule) fetchAllowList() (a *AllowList, err error) {
	/*
		var payloadBytes []byte
		if payloadBytes, err = json.Marshal(LicensePayload{LicenseKey: l.LicenseKey}); err != nil {
			return nil, err
		}
		resp, err := http.Post(licenseEndpoint, "application/json", bytes.NewReader(payloadBytes))
		defer resp.Body.Close()
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}*/
	body := []byte(`{
  "connectors": ["clickhouse", "connector-do-twojego-serca-:*"],
  "processors": ["processor1", "processor2"],
  "expiration_date": "2023-12-31T23:59:59Z"
}`)

	if err = json.Unmarshal(body, &a); err != nil {
		return nil, err
	} else {
		return a, nil
	}
}
