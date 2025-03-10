// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package end_user_errors

import (
	"errors"
	"strings"
)

func GuessClickhouseErrorType(err error) *EndUserError {
	// This limit a depth of error unwrapping.
	// We don't need forever loops especially in error handling branches
	const maxDepth = 100

	originalErr := err

	for i := 0; i < maxDepth; i++ {
		s := err.Error()

		// We should check the error type, not the string.
		// But Clickhouse doesn't provide a specific error type with details about the error.

		if strings.Contains(s, "code: 60") {
			return ErrDatabaseTableNotFound.New(originalErr)
		}

		if strings.Contains(s, "Missing columns:") {
			return ErrDatabaseFieldNotFound.New(originalErr)
		}

		if strings.HasPrefix(s, "dial tcp") {
			return ErrDatabaseConnectionError.New(originalErr)
		}

		if strings.HasPrefix(s, "code: 516") {
			return ErrDatabaseAuthenticationError.New(originalErr)
		}

		if strings.Contains(s, "unexpected packet") {
			return ErrDatabaseInvalidProtocol.New(originalErr)
		}

		if strings.Contains(s, "tls: first record does not look like a TLS handshake") {
			return ErrDatabaseTLS.New(originalErr)
		}

		if strings.Contains(s, "tls: failed to verify certificate:") {
			return ErrDatabaseTLSVerify.New(originalErr)
		}

		if strings.Contains(s, "code: 76") {
			return ErrDatabaseStorageError.New(originalErr)
		}

		err = errors.Unwrap(err)
		if err == nil {
			break
		}
	}

	return ErrDatabaseOtherError.New(originalErr)
}
