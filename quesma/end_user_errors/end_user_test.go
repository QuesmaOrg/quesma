package end_user_errors

import (
	"errors"
	"fmt"
	"testing"
)

func TestEndUserError_error_as(t *testing.T) {

	err := ErrDatabaseAuthenticationError.New(fmt.Errorf("some error"))

	var asEndUserError *EndUserError

	if !errors.As(err, &asEndUserError) {
		t.Fatal("expected error to be of type *EndUserError")
	}
}
