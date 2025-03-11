// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package quesma_errors

import "errors"

var (
	errIndexNotExists       = errors.New("table does not exist")
	errCouldNotParseRequest = errors.New("parse exception")
)

func ErrIndexNotExists() error {
	return errIndexNotExists
}

func ErrCouldNotParseRequest() error {
	return errCouldNotParseRequest
}
