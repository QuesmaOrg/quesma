// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package licensing

import (
	"fmt"
	"github.com/google/uuid"
	"os"
	"quesma/quesma/config"
	"slices"
)

type LicenseModule struct {
	InstallationID string
	LicenseKey     []byte
	License        *License
	Config         *config.QuesmaConfiguration
}

const (
	installationIdFile = ".installation_id"
)

func Init(config *config.QuesmaConfiguration) *LicenseModule {
	l := &LicenseModule{
		Config:     config,
		LicenseKey: []byte(config.LicenseKey),
	}
	l.Run()
	return l
}

func (l *LicenseModule) Run() {
	if len(l.LicenseKey) > 0 {
		fmt.Printf("License key [%s] already present, skipping license key obtainment.\n", l.LicenseKey)
	} else {
		l.setInstallationID()
		if err := l.obtainLicenseKey(); err != nil {
			PanicWithLicenseViolation(fmt.Errorf("failed to obtain license key: %v", err))
		}
	}
	if err := l.processLicense(); err != nil {
		PanicWithLicenseViolation(fmt.Errorf("failed to process license: %v", err))
	}
	if err := l.validateConfig(); err != nil {
		PanicWithLicenseViolation(fmt.Errorf("failed to validate configuration: %v", err))
	}
}

func (l *LicenseModule) validateConfig() error {
	// Check if connectors are allowed
	for _, conn := range l.Config.Connectors {
		if !slices.Contains(l.License.Connectors, conn.ConnectorType) {
			return fmt.Errorf("connector [%s] is not allowed within the current license", conn.ConnectorType)
		}
	}
	return nil
}

func (l *LicenseModule) setInstallationID() {
	if l.Config.InstallationId != "" {
		fmt.Printf("Installation ID provided in the configuration [%s]\n", l.Config.InstallationId)
		l.InstallationID = l.Config.InstallationId
		return
	}

	if data, err := os.ReadFile(installationIdFile); err != nil {
		fmt.Printf("Reading Installation ID failed [%v], generating new one\n", err)
		generatedID := uuid.New().String()
		fmt.Printf("Generated Installation ID of [%s]\n", generatedID)
		l.tryStoringInstallationIdInFile(generatedID)
		l.InstallationID = generatedID
	} else {
		installationID := string(data)
		fmt.Printf("Installation ID of [%s] found\n", installationID)
		l.InstallationID = installationID
	}
}

func (l *LicenseModule) tryStoringInstallationIdInFile(installationID string) {
	if err := os.WriteFile(installationIdFile, []byte(installationID), 0644); err != nil {
		fmt.Printf("Failed to store Installation ID in file: %v\n", err)
	} else {
		fmt.Printf("Stored Installation ID in file [%s]\n", installationIdFile)
	}
}
