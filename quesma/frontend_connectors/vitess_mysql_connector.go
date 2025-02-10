// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

// Experimental alpha processor for MySQL protocol

package frontend_connectors

import (
	"context"
	"errors"
	"time"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtenv"

	"github.com/QuesmaOrg/quesma/quesma/logger"
	"github.com/QuesmaOrg/quesma/quesma/quesma/recovery"
	quesma_api "github.com/QuesmaOrg/quesma/quesma/v2/core"
)

type VitessMySqlConnector struct {
	processors []quesma_api.Processor
	listener   *mysql.Listener
	endpoint   string
}

func NewVitessMySqlConnector(endpoint string) (*VitessMySqlConnector, error) {
	connector := VitessMySqlConnector{
		endpoint: endpoint,
	}

	// FIXME: the parameter values below should be tweaked, in particular (list not exhaustive):
	// - timeouts, delays are set to time.Second * 0
	// - authServer is set to mysql.NewAuthServerNone(), meaning no authentication
	// - TLS is not set up
	listener, err := mysql.NewListener("tcp", endpoint, mysql.NewAuthServerNone(), &connector, time.Second*0, time.Second*0, false, false, time.Second*0, time.Second*0)
	if err != nil {
		return nil, err
	}
	connector.listener = listener

	return &connector, nil
}

func (t *VitessMySqlConnector) Listen() error {
	go func() {
		defer recovery.LogPanic()
		t.listener.Accept()
	}()
	return nil
}

// Implementation of Vitess mysql.Handler interface:

type ComQueryMessage struct {
	Query string
}

func (t *VitessMySqlConnector) NewConnection(c *mysql.Conn) {
	// TODO: should we do something here?
}

func (t *VitessMySqlConnector) ConnectionReady(c *mysql.Conn) {
	// TODO: should we do something here?
}

func (t *VitessMySqlConnector) ConnectionClosed(c *mysql.Conn) {
	// TODO: should we do something here?
}

func (t *VitessMySqlConnector) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	metadata := make(map[string]interface{})
	var message any = ComQueryMessage{
		Query: query,
	}

	dispatcher := quesma_api.Dispatcher{}
	_, result := dispatcher.Dispatch(t.processors, metadata, message)
	switch result := result.(type) {
	case *sqltypes.Result:
		err := callback(result)
		if err != nil {
			return err
		}
		return nil
	case error:
		return result
	default:
		logger.Error().Msgf("Unexpected ComQuery result type received from the processor: %T", result)
		return nil
	}
}

func (t *VitessMySqlConnector) ComPrepare(c *mysql.Conn, query string, bindVars map[string]*query.BindVariable) ([]*query.Field, error) {
	// TODO implement ComPrepare
	logger.Error().Msg("ComPrepare not implemented")
	return nil, errors.New("ComPrepare not implemented")
}

func (t *VitessMySqlConnector) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	// TODO implement ComStmtExecute
	logger.Error().Msg("ComStmtExecute not implemented")
	return errors.New("ComStmtExecute not implemented")
}

func (t *VitessMySqlConnector) ComRegisterReplica(c *mysql.Conn, replicaHost string, replicaPort uint16, replicaUser string, replicaPassword string) error {
	// TODO implement ComRegisterReplica
	logger.Error().Msg("ComRegisterReplica not implemented")
	return errors.New("ComRegisterReplica not implemented")
}

func (t *VitessMySqlConnector) ComBinlogDump(c *mysql.Conn, logFile string, binlogPos uint32) error {
	// TODO implement ComBinlogDump
	logger.Error().Msg("ComBinlogDump not implemented")
	return errors.New("ComBinlogDump not implemented")
}

func (t *VitessMySqlConnector) ComBinlogDumpGTID(c *mysql.Conn, logFile string, logPos uint64, gtidSet replication.GTIDSet) error {
	// TODO implement ComBinlogDumpGTID
	logger.Error().Msg("ComBinlogDumpGTID not implemented")
	return errors.New("ComBinlogDumpGTID not implemented")
}

func (t *VitessMySqlConnector) WarningCount(c *mysql.Conn) uint16 {
	return 0
}

func (t *VitessMySqlConnector) ComResetConnection(c *mysql.Conn) {
	// TODO implement ComResetConnection
	logger.Error().Msg("ComResetConnection not implemented")
}

func (t *VitessMySqlConnector) Env() *vtenv.Environment {
	env, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: "", // will use Vitess's default version
		TruncateUILen:      512,
		TruncateErrLen:     512,
	})
	if err != nil {
		logger.Error().Msgf("failed to create environment: %v", err)
		return nil
	}

	return env
}

func (t *VitessMySqlConnector) InstanceName() string {
	return "VitessMySqlConnector"
}

func (t *VitessMySqlConnector) GetEndpoint() string {
	return t.endpoint
}

func (t *VitessMySqlConnector) Stop(ctx context.Context) error {
	t.listener.Shutdown()
	return nil
}

func (t *VitessMySqlConnector) SetHandlers(processors []quesma_api.Processor) {
	t.processors = processors
}
