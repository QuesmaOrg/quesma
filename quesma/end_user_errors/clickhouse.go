package end_user_errors

import (
	"errors"
	"fmt"
	"strings"
)

func GuessClickhouseError(err error) *EndUserError {
	fmt.Print("Guessing Clickhouse error type: " + err.Error())

	for {
		s := err.Error()

		// TODO this is stupid, but works.
		// We should check the error type, not the string.

		if strings.Contains(s, "error [code: 60") {
			return ErrDatabaseTableNotFound.NewWithErr(err)
		}

		if strings.HasPrefix(s, "dial tcp") {
			return ErrDatabaseConnectionError.NewWithErr(err)
		}

		if strings.HasPrefix(s, "code: 516") {
			return ErrDatabaseAuthenticationError.NewWithErr(err)
		}

		err = errors.Unwrap(err)
		if err == nil {
			break
		}
	}

	return ErrDatabaseOtherError.NewWithErr(err)
}
