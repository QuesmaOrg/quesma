package end_user_errors

import (
	"fmt"
)

// EndUserError This represents an error that can be shown to the end user.
type EndUserError struct {
	errorType       *ErrorType
	details         string
	internalDetails string
	originError     error // this can be nil
}

// Error this is the error interface implementation. This message is logged in the logs.
func (e *EndUserError) Error() string {

	details := ""

	if e.internalDetails != "" {
		details = fmt.Sprintf("%s %s", details, e.internalDetails)
	}

	if e.details != "" {
		details += e.details
	}

	if e.originError == nil {
		return fmt.Sprintf("%s %s", e.errorType.String(), details)
	} else {
		return fmt.Sprintf("%s %s: %s", e.errorType.String(), details, e.originError)
	}
}

// Reason returns message logged in to reason field
func (e *EndUserError) Reason() string {
	return e.errorType.Message
}

// EndUserErrorMessage returns the error message that can be shown to the end user.
func (e *EndUserError) EndUserErrorMessage() string {
	return fmt.Sprintf("%s%s", e.errorType.String(), e.details)
}

// Details sets details about the error. It will be available for end user.
func (e *EndUserError) Details(format string, args ...any) *EndUserError {
	e.details = fmt.Sprintf(format, args...)
	return e
}

// InternalDetails sets our internal details.
func (e *EndUserError) InternalDetails(format string, args ...any) *EndUserError {
	e.internalDetails = fmt.Sprintf(format, args...)
	return e
}

type ErrorType struct {
	Number  int
	Message string
}

func (t *ErrorType) String() string {
	return fmt.Sprintf("Q%04d: %s", t.Number, t.Message)
}

// New create an error instance based on the error type.
func (t *ErrorType) New(err error) *EndUserError {
	return &EndUserError{
		errorType:   t,
		originError: err,
	}
}

func errorType(number int, message string) *ErrorType {
	return &ErrorType{Number: number, Message: message}
}

// Error type numbers follow the pattern QXXXX
// Where
// Q1XXX - Preprocessing errors (related to HTTP requests, JSON parsing, etc.)
// Q2XXX - Query processing errors. Query translation etc.
// Q3XXX - Errors related to external storages like Clickhouse, Elasticsearch, etc.
// Q4XXX - Errors related to other internal components telemetry, etc.

var ErrSearchCondition = errorType(2001, "Not supported search condition.")
var ErrNoSuchTable = errorType(2002, "Missing table.")

var ErrDatabaseTableNotFound = errorType(3001, "Table not found in database.")
var ErrDatabaseFieldNotFound = errorType(3002, "Field not found in database.")
var ErrDatabaseConnectionError = errorType(3003, "Error connecting to database.")
var ErrDatabaseQueryError = errorType(3004, "Error executing query in database.")
var ErrDatabaseAuthenticationError = errorType(3005, "Error authenticating with database.")
var ErrDatabaseOtherError = errorType(3006, "Unspecified database error.")
