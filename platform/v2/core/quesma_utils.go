// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_api

import (
	"fmt"
)

type Cloner interface {
	Clone() Cloner
}

func CheckedCast[T any](value interface{}) (T, error) {
	v, ok := value.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("cannot cast %v to %T", value, zero)
	}
	return v, nil
}

func SetInputType[T any]() any {
	return nil
}
