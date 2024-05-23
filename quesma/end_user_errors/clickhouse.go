package end_user_errors

import (
	"errors"
	"fmt"
	"strings"
)

func GuessClickhouseError(err error) *EndUserError {
	fmt.Print("Guessing Clickhouse error type: " + err.Error())

	for {
		msg := err.Error()

		if strings.Contains(msg, "clickhouse") {
			return ErrClickhouseQueryError.NewWithErr(err)
		}

		err = errors.Unwrap(err)
		if err == nil {
			break
		}
	}

	return ErrClickhouseQueryError.NewWithErr(err)
}
