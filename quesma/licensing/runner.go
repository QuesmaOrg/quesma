// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package licensing

import (
	"fmt"
	"github.com/google/uuid"
	"os"
	"quesma/logger"
	"quesma/quesma/config"
	"slices"
)

type LicenseModule struct {
	InstallationID string
	LicenseKey     []byte
	AllowList      *AllowList
	Config         *config.QuesmaConfiguration
}

const (
	installationIdFile = ".installation_id"
)

func Init(config *config.QuesmaConfiguration) *LicenseModule {
	l := &LicenseModule{
		Config: config,
	}
	l.setInstallationID()
	go l.Run()
	return l
}

func (l *LicenseModule) Run() {
	if err := l.obtainLicenseKey(); err != nil {
		PanicWithLicenseViolation(fmt.Errorf("failed to obtain license key: %v", err))
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
		if !slices.Contains(l.AllowList.Connectors, conn.ConnectorType) {
			return fmt.Errorf("connector [%s] is not allowed within the current license", conn.ConnectorType)
		}
	}
	return nil
}

func (l *LicenseModule) setInstallationID() {
	if l.Config.InstallationId != "" {
		logger.Info().Msgf("Installation ID provided in the configuration [%s]", l.Config.InstallationId)
		l.InstallationID = l.Config.InstallationId
		return
	}

	if data, err := os.ReadFile(installationIdFile); err != nil {
		logger.Info().Msgf("Reading Installation ID failed [%v], generating new one", err)
		generatedID := uuid.New().String()
		logger.Info().Msgf("Generated Installation ID of [%s]", generatedID)
		l.tryStoringInstallationIdInFile(generatedID)
		l.InstallationID = generatedID
	} else {
		installationID := string(data)
		logger.Info().Msgf("Installation ID of [%s] found", installationID)
		l.InstallationID = installationID
	}
}

func (l *LicenseModule) tryStoringInstallationIdInFile(installationID string) {
	if err := os.WriteFile(installationIdFile, []byte(installationID), 0644); err != nil {
		logger.Warn().Msgf("Failed to store Installation ID in file: %v", err)
	} else {
		logger.Info().Msgf("Stored Installation ID in file [%s]", installationIdFile)
	}
}
