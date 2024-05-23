package end_user_errors

import (
	"errors"
	"strings"
)

func GuessClickhouseErrorType(err error) *EndUserError {

	for {
		s := err.Error()

		// TODO this is stupid, but works.
		// We should check the error type, not the string.

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
