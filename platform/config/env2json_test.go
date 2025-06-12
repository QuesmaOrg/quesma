// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"github.com/goccy/go-json"
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
	t.Cleanup(func() {
		os.Unsetenv("ENV2JSON_licenseKey")
		os.Unsetenv("ENV2JSON_backendConnectors_0_config_url")
		os.Unsetenv("ENV2JSON_backendConnectors_0_config_user")
		os.Unsetenv("ENV2JSON_backendConnectors_0_config_password")
	})
	resultJson, err := provider.ReadBytes()
	assert.NoError(t, err)

	expectedJson := `{"licenseKey":"secret_key","backendConnectors":[{"config":{"url":"http://localhost:8080","user":"user","password":"password"}}]}`
	assert.Equal(t, expectedJson, string(resultJson))
}

func TestEnv2Json_arraysByName(t *testing.T) {
	os.Setenv(configFileLocationEnvVar, "./test_configs/test_config_v2.yaml")
	os.Setenv("QUESMA_licenseKey", "secret_key")
	os.Setenv("QUESMA_backendConnectors_my-clickhouse-data-source_config_url", "http://localhost:9201")
	os.Setenv("QUESMA_backendConnectors_my-clickhouse-data-source_config_user", "user")
	os.Setenv("QUESMA_backendConnectors_my-clickhouse-data-source_config_password", "password")
	t.Cleanup(func() {
		os.Unsetenv("QUESMA_licenseKey")
		os.Unsetenv("QUESMA_backendConnectors_my-clickhouse-data-source_config_url")
		os.Unsetenv("QUESMA_backendConnectors_my-clickhouse-data-source_config_user")
		os.Unsetenv("QUESMA_backendConnectors_my-clickhouse-data-source_config_password")
	})

	cfg, err := LoadV2Config()
	assert.NoError(t, err)
	assert.Len(t, cfg.BackendConnectors, 2)
	clickHouseBackend := cfg.BackendConnectors[1]
	assert.Equal(t, "my-clickhouse-data-source", clickHouseBackend.Name)
	assert.Equal(t, "http://localhost:9201", clickHouseBackend.Config.Url.String())
	assert.Equal(t, "user", clickHouseBackend.Config.User)
	assert.Equal(t, "password", clickHouseBackend.Config.Password)
}

func TestEnv2Json_empty(t *testing.T) {
	provider := Env2JsonProvider("ENV2JSON2_", "_", nil)
	resultJson, err := provider.ReadBytes()
	assert.NoError(t, err)

	expectedJson := `{}`
	assert.Equal(t, expectedJson, string(resultJson))
}

func TestEnv2Json_jsonMerge(t *testing.T) {
	jsonA := `{"a":1,"b":2,"c":[{"d":1},{"d":2},{"d":3}]}`
	jsonB := `{"a":3,"l":2,"c":[null,{"e":42}]}`
	// turn into dicts
	var dictA map[string]interface{}
	var dictB map[string]interface{}
	err := json.Unmarshal([]byte(jsonA), &dictA)
	assert.NoError(t, err)
	err = json.Unmarshal([]byte(jsonB), &dictB)
	assert.NoError(t, err)

	err = mergeDictFunc(dictA, dictB)
	assert.NoError(t, err)
	mergedJson, err2 := json.Marshal(dictB)
	assert.NoError(t, err2)
	expectedJson := `{"a":1,"b":2,"c":[{"d":1},{"d":2,"e":42},{"d":3}],"l":2}`
	assert.Equal(t, expectedJson, string(mergedJson))
}
