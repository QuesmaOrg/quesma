// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package types

import (
	"github.com/goccy/go-json"
	"fmt"
	"github.com/barkimedes/go-deepcopy"
	"github.com/tailscale/hujson"
)

type JSON map[string]interface{}

func ParseJSON(body string) (JSON, error) {

	var res JSON
	err := json.Unmarshal([]byte(body), &res)
	if err != nil {
		if newBytes, errStd := hujson.Standardize([]byte(body)); errStd == nil {
			err = json.Unmarshal(newBytes, &res)
		}
	}

	return res, err
}

// Parses JSON and panics if it fails. This is useful for tests only.
func MustJSON(s string) JSON {

	res, err := ParseJSON(s)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse JSON: %v", err))
	}

	return res
}

func (j JSON) Bytes() ([]byte, error) {
	return json.Marshal(j)
}

func (j JSON) Remarshal(v interface{}) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func (j JSON) ShortString() string {

	var asString string
	asBytes, err := json.Marshal(j)

	if err != nil {
		asString = fmt.Sprintf("Error marshalling JSON: %v, json: %v", err, j)
	} else {
		asString = string(asBytes)
	}

	if len(asString) < 70 {
		return asString
	}
	return asString[:70]

}

func (j JSON) Clone() JSON {
	cloned, err := deepcopy.Anything(j)
	if err != nil {
		panic(fmt.Sprintf("Failed to clone JSON, err: %v, json: %v", err, j))
	}
	return cloned.(JSON)
}
