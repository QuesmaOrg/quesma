package end_user_errors

import (
	"errors"
	"testing"
)

func TestEndUserError_error_as(t *testing.T) {

	err := ErrClickhouseAuthError.New()

	var asEndUserError *EndUserError

	if !errors.As(err, &asEndUserError) {
		t.Fatal("expected error to be of type *EndUserError")
	}
}
