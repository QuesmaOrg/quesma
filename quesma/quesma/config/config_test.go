// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestIndexConfiguration_FullTextField(t *testing.T) {

	indexConfig := map[string]IndexConfiguration{
		"none": {
			Name:           "none",
			Enabled:        true,
			FullTextFields: []string{},
		},
		"foo-bar": {
			Name:           "foo-bar",
			Enabled:        true,
			FullTextFields: []string{"sometext"},
		},
		"bar-logs": {
			Name:           "bar-logs",
			Enabled:        true,
			FullTextFields: []string{},
		},
		"logs-generic-default": {
			Name:           "logs-generic-default",
			Enabled:        true,
			FullTextFields: []string{"message", "content"},
		},
	}

	cfg := QuesmaConfiguration{IndexConfig: indexConfig}

	tests := []struct {
		name      string
		indexName string
		fieldName string
		want      bool
	}{
		{"has full text field", "logs-generic-default", "message", true},
		{"has full text field", "logs-generic-default", "content", true},
		{"dont have full text field", "foo-bar", "content", false},
		{"dont have full text field", "bar-logs", "content", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, cfg.IsFullTextMatchField(tt.indexName, tt.fieldName), "IsFullTextMatchField(%parsedViper, %parsedViper)", tt.indexName, tt.fieldName)
		})
	}

}

func TestQuesmaConfigurationLoading(t *testing.T) {

	os.Setenv(configFileLocationEnvVar, "./test_config.yaml")

	logLevelPassedAsEnvVar := "debug"
	licenseKeyPassedAsEnvVar := "arbitraty-license-key"
	os.Setenv("QUESMA_logging_level", logLevelPassedAsEnvVar) // overrides what's in the config file
	os.Setenv("QUESMA_licenseKey", licenseKeyPassedAsEnvVar)  // overrides what's in the config file
	cfg := Load()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("error validating config: %v", err)
	}

	assert.Equal(t, licenseKeyPassedAsEnvVar, cfg.LicenseKey)
	assert.Equal(t, DualWriteQueryClickhouse, cfg.Mode)
	assert.Equal(t, 8080, int(cfg.PublicTcpPort))
	assert.Equal(t, "http://localhost:9200", cfg.Elasticsearch.Url.String())
	assert.Equal(t, false, cfg.Elasticsearch.Call)
	assert.Equal(t, "clickhouse://localhost:9000", cfg.ClickHouse.Url.String())
	assert.Equal(t, true, cfg.IngestStatistics)
	assert.Equal(t, "logs", cfg.Logging.Path)
	assert.Equal(t, logLevelPassedAsEnvVar, cfg.Logging.Level.String())
	assert.Equal(t, 10, len(cfg.IndexConfig))

	findIndexConfig := func(name string) *IndexConfiguration {
		if configuration, found := cfg.IndexConfig[name]; found {
			return &configuration
		} else {
			return nil
		}
	}

	tests := []struct {
		name           string
		enabled        bool
		fullTextFields []string
	}{
		{"logs-generic-default", true, []string{"message", "host.name"}},
		{"device-logs", true, []string{"message"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ic := findIndexConfig(tt.name)
			assert.NotNil(t, ic)
			assert.Equal(t, tt.enabled, ic.Enabled)
			assert.Equal(t, tt.fullTextFields, ic.FullTextFields)
		})
	}
}

func TestClickHouseAndHydrolixConfigurationMutuallyExclusive(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_config.yaml")

	os.Setenv("QUESMA_hydrolix_url", "clickhouse://hydrolixhost.org:8080")
	cfg := Load()

	var validationErr error
	err := cfg.Validate()

	validationErr = multierror.Append(validationErr, fmt.Errorf("only one of ClickHouse and Hydrolix can be configured"))
	if multiErr, ok := err.(*multierror.Error); !ok {
		t.Errorf("Expected a multierror, got: %v", err)
	} else {
		assert.Equal(t, 1, multiErr.Len())
		assert.Contains(t, multiErr.Errors, fmt.Errorf("only one of ClickHouse and Hydrolix can be configured"))
	}

	assert.Equal(t, "clickhouse://hydrolixhost.org:8080", cfg.ClickHouse.Url.String())
}

func TestHydrolixConfigurationLandsInClickHouseConfig(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_config.yaml")

	os.Setenv("QUESMA_hydrolix_url", "clickhouse://hydrolixhost.org:8080")
	os.Setenv("QUESMA_hydrolix_user", "user")
	os.Setenv("QUESMA_hydrolix_password", "pass")
	os.Setenv("QUESMA_hydrolix_database", "dbname")
	cfg := Load()

	var validationErr error
	err := cfg.Validate()

	validationErr = multierror.Append(validationErr, fmt.Errorf("only one of ClickHouse and Hydrolix can be configured"))
	if multiErr, ok := err.(*multierror.Error); !ok {
		t.Errorf("Expected a multierror, got: %v", err)
	} else {
		assert.Equal(t, 1, multiErr.Len())
		assert.Contains(t, multiErr.Errors, fmt.Errorf("only one of ClickHouse and Hydrolix can be configured"))
	}

	assert.Equal(t, "clickhouse://hydrolixhost.org:8080", cfg.ClickHouse.Url.String())
	assert.Equal(t, "user", cfg.ClickHouse.User)
	assert.Equal(t, "pass", cfg.ClickHouse.Password)
	assert.Equal(t, "dbname", cfg.ClickHouse.Database)
}

func TestMatchName(t *testing.T) {
	type args struct {
		indexName        string
		indexNamePattern string
	}
	tests := []struct {
		args args
		want bool
	}{
		{args: args{"logs-generic-default", "logs-generic-default*"}, want: true},
		{args: args{"logs-generic-default", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-default-foo", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-", "logs-generic-*"}, want: true},
		{args: args{"logs-generic", "logs-generic-*"}, want: false},
		{args: args{"logs2-generic", "logs-generic-*"}, want: false},
		{args: args{"logs-generic-default", "logs-*-default"}, want: true},
		{args: args{"logs-specific", "logs-generic-*"}, want: false},
		{args: args{"logs-generic-123", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-default-foo-bar", "logs-generic-*"}, want: true},
		{args: args{"logs-generic-abc", "logs-generic-*"}, want: true},
		{args: args{"logs-custom-default", "logs-*-default"}, want: true},
		{args: args{"logs-custom-default", "logs-generic-*"}, want: false},
		{args: args{"logs-custom-specific", "logs-custom-*"}, want: true},
		{args: args{"logs-custom-specific-123", "logs-custom-*"}, want: true},
		{args: args{"logs-custom-abc", "logs-custom-*"}, want: true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s->%s[%v]", tt.args.indexName, tt.args.indexNamePattern, tt.want), func(t *testing.T) {
			assert.Equalf(t, tt.want, MatchName(tt.args.indexNamePattern, tt.args.indexName), "matches(%v, %v)", tt.args.indexName, tt.args.indexNamePattern)
		})
	}
}
