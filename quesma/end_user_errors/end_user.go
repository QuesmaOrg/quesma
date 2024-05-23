package end_user_errors

import (
	"fmt"
)

// EndUserError This represents an error that can be shown to the end user.
type EndUserError struct {
	msg             *ErrorMessage
	details         string
	internalDetails string
	originError     error
}

func (e *EndUserError) Error() string {
	return fmt.Sprintf("EndUserError: Q%04d: %s: %s:  %s", e.msg.Number, e.msg.Message+e.details, e.internalDetails, e.originError)
}

func (e *EndUserError) Reason() string {
	return fmt.Sprintf("Q%04d: %s", e.msg.Number, e.msg.Message+e.details)
}

// EndUserErrorMessage returns the error message that can be shown to the end user.
func (e *EndUserError) EndUserErrorMessage() string {
	return fmt.Sprintf("Q%04d: %s", e.msg.Number, e.msg.Message+e.details)
}

func (e *EndUserError) Details(format string, args ...any) *EndUserError {
	e.details = fmt.Sprintf(format, args...)
	return e
}

func (e *EndUserError) InternalDetails(format string, args ...any) *EndUserError {
	e.internalDetails = fmt.Sprintf(format, args...)
	return e
}

type ErrorMessage struct {
	Number  int
	Message string
}

func (m *ErrorMessage) NewWithErr(originError error) *EndUserError {

	return &EndUserError{
		msg:         m,
		originError: originError,
	}
}

func (m *ErrorMessage) New() *EndUserError {
	return &EndUserError{
		msg: m,
	}
}

func msg(number int, message string) *ErrorMessage {
	return &ErrorMessage{Number: number, Message: message}
}

// Error numbers follow the pattern QXXXX
// Where
// Q1XXX - Preprocessing errors (related to HTTP requests, JSON parsing, etc.)
// Q2XXX - Query processing errors. Query translation etc.
// Q3XXX - Errors related to external storages like Clickhouse, Elasticsearch, etc.
// Q4XXX - Errors related to other internal components telemetry, etc.

var ErrQueryElasticAndQuesma = msg(2001, "Querying data in Elasticsearch and Clickhouse is not supported at the moment.")
var ErrNoSuchTable = msg(2002, "Missing table.")

var ErrClickhouseTableNotFound = msg(3001, "Table not found in Clickhouse")
var ErrClickhouseFieldNotFound = msg(3002, "Field not found in Clickhouse")
var ErrClickhouseConnectionError = msg(3003, "Error connecting to Clickhouse")
var ErrClickhouseQueryError = msg(3004, "Error executing query in Clickhouse")
var ErrClickhouseAuthError = msg(3005, "Error authenticating with Clickhouse")
