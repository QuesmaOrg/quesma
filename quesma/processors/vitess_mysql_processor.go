// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// Experimental alpha processor for MySQL protocol

package processors

import (
	"context"
	"errors"
	"fmt"
	"github.com/QuesmaOrg/quesma/quesma/logger"
	"reflect"
	"time"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"

	"github.com/QuesmaOrg/quesma/quesma/backend_connectors"
	"github.com/QuesmaOrg/quesma/quesma/frontend_connectors"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
)

type VitessMySqlProcessor struct {
	BaseProcessor
}

func (t VitessMySqlProcessor) InstanceName() string {
	return "VitessMySqlProcessor"
}

func (t VitessMySqlProcessor) GetId() string {
	return "VitessMySqlProcessor"
}

func (t *VitessMySqlProcessor) Handle(metadata map[string]interface{}, messages ...any) (map[string]interface{}, any, error) {
	if len(messages) != 1 {
		return metadata, nil, errors.New("expected exactly one message")
	}

	switch message := messages[0].(type) {
	case frontend_connectors.ComQueryMessage:
		return metadata, t.processComQuery(message), nil
	default:
		logger.Error().Msgf("Unsupported message received by VitessMySqlProcessor: %v (of type %T)", message, message)
		return metadata, nil, fmt.Errorf("unsupported message type: %T", message)
	}

}

func (t *VitessMySqlProcessor) processComQuery(message frontend_connectors.ComQueryMessage) any {
	// TODO: this should return proper MySQL errors, not just errors.New()
	// which don't get translated to proper MySQL error codes

	logger.Info().Msgf("Received ComQuery message: %s", message.Query)

	// TODO: support other SQL backends, design a better Processor API for that
	backendConn := t.GetBackendConnector(quesma_api.MySQLBackend)
	if backendConn == nil {
		backendConn = t.GetBackendConnector(quesma_api.PgSQLBackend)
	}
	if backendConn == nil {
		return errors.New("no backend connector found")
	}

	err := backendConn.Open()
	if err != nil {
		return fmt.Errorf("error opening connection: %v", err)
	}
	defer backendConn.Close()

	backendConnSql := backendConn.(backend_connectors.SqlBackendConnector).GetDB()

	rows, err := backendConnSql.QueryContext(context.Background(), message.Query)
	if err != nil {
		logger.Error().Msgf("Failed to QueryContext: %v (%s)", err, message.Query)
		return fmt.Errorf("failed to QueryContext: %v", err)
	}
	defer rows.Close()

	// TODO: this structure is not yet filled completely correctly
	result := sqltypes.Result{
		Fields:              nil,
		RowsAffected:        1,
		InsertID:            1,
		Rows:                nil,
		SessionStateChanges: "",
		StatusFlags:         0,
		Info:                "",
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		logger.Error().Msgf("Failed to get column types: %v (%s)", err, message.Query)
		return fmt.Errorf("failed to get column types: %v", err)
	}

	for _, columnType := range columnTypes {
		typ, err := ConvertToVitessType(columnType.ScanType())
		if err != nil {
			logger.Error().Msgf("Failed to convert column type: %v (%s)", err, columnType.Name())
			return fmt.Errorf("failed to convert column type: %v", err)
		}

		// TODO: this structure is not yet filled completely correctly
		result.Fields = append(result.Fields, &query.Field{
			Name:         columnType.Name(),
			Type:         typ,
			Table:        "",
			OrgTable:     "",
			Database:     "",
			OrgName:      "",
			ColumnLength: 0,
			Charset:      0,
			Decimals:     0,
			Flags:        0,
			ColumnType:   typ.String(),
		})
	}

	for rows.Next() {
		values := make([]interface{}, len(columnTypes))
		scanArgs := make([]interface{}, len(columnTypes))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			logger.Error().Msgf("Failed to execute scan row: %v (%s)", err, message.Query)
			continue
		}

		row := make([]sqltypes.Value, len(columnTypes))
		for i, v := range values {
			val, err := ConvertToVitessValue(v)
			if err != nil {
				logger.Error().Msgf("Error converting value: %v", err)
				continue
			}
			row[i] = val
		}

		result.Rows = append(result.Rows, row)
	}

	if err = rows.Err(); err != nil {
		logger.Error().Msgf("Query resulted in an error: %v (%s)", err, message.Query)
		return fmt.Errorf("row iteration error: %v", err)
	}

	return &result
}

func ConvertToVitessValue(goval any) (sqltypes.Value, error) {
	switch goval := goval.(type) {
	case nil:
		return sqltypes.NULL, nil
	case time.Time:
		return sqltypes.NewDatetime(goval.Format("2006-01-02 15:04:05")), nil
	case []byte:
		return sqltypes.MakeTrusted(sqltypes.VarBinary, goval), nil
	case int64:
		return sqltypes.NewInt64(goval), nil
	case uint64:
		return sqltypes.NewUint64(goval), nil
	case float64:
		return sqltypes.NewFloat64(goval), nil
	case string:
		return sqltypes.NewVarChar(goval), nil
	default:
		return sqltypes.NULL, fmt.Errorf("unexpected type %T: %v", goval, goval)
	}
}
func ConvertToVitessType(goval reflect.Type) (sqltypes.Type, error) {
	switch goval.Kind() {
	case reflect.Invalid:
		return sqltypes.Null, nil
	case reflect.Slice:
		if goval.Elem().Kind() == reflect.Uint8 {
			return sqltypes.VarBinary, nil
		}
	case reflect.Int32:
		return sqltypes.Int32, nil
	case reflect.Int64:
		return sqltypes.Int64, nil
	case reflect.Uint64:
		return sqltypes.Uint64, nil
	case reflect.Float64:
		return sqltypes.Float64, nil
	case reflect.String:
		return sqltypes.VarChar, nil
	case reflect.Struct:
		switch goval.String() {
		case "sql.NullTime", "time.Time":
			return sqltypes.Datetime, nil
		case "sql.NullString":
			return sqltypes.VarChar, nil
		}
		return sqltypes.Null, fmt.Errorf("unexpected struct type %v", goval)
	default:
		return sqltypes.Null, fmt.Errorf("unexpected type %v", goval)
	}
	return sqltypes.Null, fmt.Errorf("unexpected type %v", goval)
}

func NewVitessMySqlProcessor() *VitessMySqlProcessor {
	return &VitessMySqlProcessor{
		BaseProcessor: NewBaseProcessor(),
	}
}
