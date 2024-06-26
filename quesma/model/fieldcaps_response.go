// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package model

// https://github.com/elastic/go-elasticsearch/blob/main/typedapi/core/fieldcaps/response.go#L35
type FieldCapsResponse struct {
	Fields  map[string]map[string]FieldCapability `json:"fields"`
	Indices []string                              `json:"indices"`
}
