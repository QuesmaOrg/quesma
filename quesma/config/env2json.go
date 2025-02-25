// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package config

import (
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/tidwall/sjson"
	"os"
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

func mergeArrayFunc(src, dest []interface{}) ([]interface{}, error) {
	newLen := len(src)
	if len(dest) > newLen {
		newLen = len(dest)
	}
	newArray := make([]interface{}, newLen)

	for i := 0; i < newLen; i++ {
		if i >= len(src) {
			newArray[i] = dest[i]
		} else if i >= len(dest) {
			newArray[i] = src[i]
		} else if src[i] == nil {
			newArray[i] = dest[i]
		} else if dest[i] == nil {
			newArray[i] = src[i]
		} else {
			if srcMap, isMap := src[i].(map[string]interface{}); isMap {
				if destMap, isDestMap := dest[i].(map[string]interface{}); isDestMap {
					if err := mergeDictFunc(srcMap, destMap); err != nil {
						return nil, err
					}
					newArray[i] = destMap
					continue
				}
			}

			newArray[i] = src[i]
		}
	}

	return newArray, nil
}

func mergeDictIntoArrayFunc(src map[string]interface{}, dest []interface{}) ([]interface{}, error) {
	newArray := make([]interface{}, len(dest))
	copy(newArray, dest)
	for k, v := range src {
		foundIdx := -1

		// find existing element with same name
		for i := range newArray {
			if m, isMap := newArray[i].(map[string]interface{}); isMap {
				if m["name"] == k {
					foundIdx = i
					break
				}
			}
		}

		// if not exist add new element
		if foundIdx == -1 {
			foundIdx = len(newArray)
			newMap := make(map[string]interface{})
			newMap["name"] = k
			newArray = append(newArray, newMap)
		}

		if m, isMap := newArray[foundIdx].(map[string]interface{}); isMap {
			if vTyped, isMap2 := v.(map[string]interface{}); isMap2 {
				if err := mergeDictFunc(vTyped, m); err != nil {
					return nil, err
				}
				newArray[foundIdx] = m
				continue
			}
		}
		newArray[foundIdx] = v
	}
	return newArray, nil
}

func mergeDictFunc(src, dest map[string]interface{}) error {
	for k, v := range src {
		switch vTyped := v.(type) {
		case map[string]interface{}:
			if destV, exist := dest[k]; exist {
				if destMap, isMap := destV.(map[string]interface{}); isMap {
					if err := mergeDictFunc(vTyped, destMap); err != nil {
						return err
					}
					continue
				} else if destArray, isArray := destV.([]interface{}); isArray {
					if newV, err := mergeDictIntoArrayFunc(vTyped, destArray); err != nil {
						return err
					} else {
						dest[k] = newV
					}
					continue
				}
			}
			dest[k] = v
		case []interface{}:
			if destV, exist := dest[k]; exist {
				if destMap, isArray := destV.([]interface{}); isArray {
					if newV, err := mergeArrayFunc(vTyped, destMap); err != nil {
						return err
					} else {
						dest[k] = newV
					}
					continue
				}
			}
			dest[k] = v
		default:
			dest[k] = v
		}
	}
	return nil
}
