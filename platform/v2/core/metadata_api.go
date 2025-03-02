// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import "strconv"

func MakeNewMetadata() map[string]any {
	return make(map[string]any)
}

func SetCorrelationId(metadata map[string]any, correlationId int64) {
	metadata["correlationId"] = strconv.FormatInt(correlationId, 10)
}

func GetCorrelationId(metadata map[string]any) string {
	if correlationId, ok := metadata["correlationId"]; !ok {
		panic("CorrelationId not found in metadata")
	} else {
		checkedCorrelationId, err := CheckedCast[string](correlationId)
		if err != nil {
			panic("CorrelationId is not string")
		}
		return checkedCorrelationId
	}
}
