// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"errors"
	"fmt"
	"github.com/tidwall/sjson"
	"os"
	"quesma/logger"
	"strings"
)

type Env2Json struct {
	prefix     string
	separator  string
	callback   func(key string, value string) (string, interface{})
	resultJson string
}

func Env2JsonProvider(prefix, sep string, callback func(key string, value string) (string, interface{})) *Env2Json {
	if len(prefix) == 0 || len(sep) == 0 {
		logger.Error().Msgf("Env2JsonProvider: prefix '%s' and sep '%s' is required", prefix, sep)
		return nil
	}
	if callback == nil {
		callback = func(key string, value string) (string, interface{}) {
			return key, value
		}
	}
	e := &Env2Json{
		prefix:     prefix,
		separator:  sep,
		resultJson: "{}",
		callback:   callback,
	}
	return e
}

func (e *Env2Json) ReadBytes() ([]byte, error) {
	var envKeyValues []string
	for _, keyValue := range os.Environ() {
		if strings.HasPrefix(keyValue, e.prefix) {
			envKeyValues = append(envKeyValues, strings.TrimPrefix(keyValue, e.prefix))
		}
	}

	for _, keyValue := range envKeyValues {
		parts := strings.SplitN(keyValue, "=", 2)
		if len(parts) != 2 {
			return []byte{}, fmt.Errorf("invalid environment variable '%s', no '='", keyValue)
		}
		key, value := e.callback(parts[0], parts[1])
		// Omit blank keys
		if key == "" {
			continue
		}

		if err := e.set(key, value); err != nil {
			return []byte{}, err
		}
	}

	return []byte(e.resultJson), nil
}

func (e *Env2Json) set(key string, value interface{}) error {
	resultJson, err := sjson.Set(e.resultJson, strings.Replace(key, e.separator, ".", -1), value)
	if err == nil {
		e.resultJson = resultJson
	}

	return err
}

func (e *Env2Json) Read() (map[string]interface{}, error) {
	return nil, errors.New("env2json Provider does not support Read()")
}
