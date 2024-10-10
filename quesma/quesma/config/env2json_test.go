// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestEnv2Json_arrays(t *testing.T) {
	provider := Env2JsonProvider("ENV2JSON_", "_", nil)
	os.Setenv("ENV2JSON_licenseKey", "secret_key")
	os.Setenv("ENV2JSON_backendConnectors_0_config_url", "http://localhost:8080")
	os.Setenv("ENV2JSON_backendConnectors_0_config_user", "user")
	os.Setenv("ENV2JSON_backendConnectors_0_config_password", "password")
	resultJson, err := provider.ReadBytes()
	assert.NoError(t, err)

	expectedJson := `{"licenseKey":"secret_key","backendConnectors":[{"config":{"url":"http://localhost:8080","user":"user","password":"password"}}]}`
	assert.Equal(t, expectedJson, string(resultJson))
}

func TestEnv2Json_empty(t *testing.T) {
	provider := Env2JsonProvider("ENV2JSON2_", "_", nil)
	resultJson, err := provider.ReadBytes()
	assert.NoError(t, err)

	expectedJson := `{}`
	assert.Equal(t, expectedJson, string(resultJson))
}
