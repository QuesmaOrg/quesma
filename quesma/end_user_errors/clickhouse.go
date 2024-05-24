package end_user_errors

import (
	"errors"
	"strings"
)

func GuessClickhouseErrorType(err error) *EndUserError {

	// This limit a depth of error unwrapping.
	// We don't need forever loops especially in error handling branches
	const maxDepth = 100

	for i := 0; i < maxDepth; i++ {
		s := err.Error()

		// We should check the error type, not the string.
		// But Clickhouse doesn't provide a specific error type with details about the error.

		if strings.Contains(s, "code: 60") {
			return ErrDatabaseTableNotFound.New(err)
		}

		if strings.HasPrefix(s, "dial tcp") {
			return ErrDatabaseConnectionError.New(err)
		}

		if strings.HasPrefix(s, "code: 516") {
			return ErrDatabaseAuthenticationError.New(err)
		}

		err = errors.Unwrap(err)
		if err == nil {
			break
		}
	}

	return ErrDatabaseOtherError.New(err)
}
