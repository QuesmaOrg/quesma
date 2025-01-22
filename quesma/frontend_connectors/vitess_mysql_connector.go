// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package frontend_connectors

import (
	"context"
	"github.com/QuesmaOrg/quesma/quesma/quesma/recovery"
	"log"
	"time"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtenv"
)

type VitessMySqlConnector struct {
	listener *mysql.Listener
	endpoint string
}

func NewVitessMySqlConnector(endpoint string) (*VitessMySqlConnector, error) {
	// FIXME: parameters values are not well thought out
	connector := VitessMySqlConnector{
		endpoint: endpoint,
	}

	listener, err := mysql.NewListener("tcp", endpoint, nil, &connector, time.Second*0, time.Second*0, false, false, time.Second*0, time.Second*0)
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

func (t *VitessMySqlConnector) NewConnection(c *mysql.Conn) {
	//TODO implement me
	panic("implement me: NewConnection")
}

func (t *VitessMySqlConnector) ConnectionReady(c *mysql.Conn) {
	//TODO implement me
	panic("implement me: ConnectionReady")
}

func (t *VitessMySqlConnector) ConnectionClosed(c *mysql.Conn) {
	//TODO implement me
	panic("implement me: ConnectionClosed")
}

func (t *VitessMySqlConnector) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	//TODO implement me
	panic("implement me: ComQuery")
}

func (t *VitessMySqlConnector) ComPrepare(c *mysql.Conn, query string, bindVars map[string]*query.BindVariable) ([]*query.Field, error) {
	//TODO implement me
	panic("implement me: ComPrepare")
}

func (t *VitessMySqlConnector) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	//TODO implement me
	panic("implement me: ComStmtExecute")
}

func (t *VitessMySqlConnector) ComRegisterReplica(c *mysql.Conn, replicaHost string, replicaPort uint16, replicaUser string, replicaPassword string) error {
	//TODO implement me
	panic("implement me: ComRegisterReplica")
}

func (t *VitessMySqlConnector) ComBinlogDump(c *mysql.Conn, logFile string, binlogPos uint32) error {
	//TODO implement me
	panic("implement me: ComBinlogDump")
}

func (t *VitessMySqlConnector) ComBinlogDumpGTID(c *mysql.Conn, logFile string, logPos uint64, gtidSet replication.GTIDSet) error {
	//TODO implement me
	panic("implement me: ComBinlogDumpGTID")
}

func (t *VitessMySqlConnector) WarningCount(c *mysql.Conn) uint16 {
	//TODO implement me
	panic("implement me: WarningCount")
}

func (t *VitessMySqlConnector) ComResetConnection(c *mysql.Conn) {
	//TODO implement me
	panic("implement me: ComResetConnection")
}

func (t *VitessMySqlConnector) Env() *vtenv.Environment {
	env, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: "", // will use Vitess's default version
		TruncateUILen:      512,
		TruncateErrLen:     512,
	})
	if err != nil {
		log.Fatalf("failed to create environment: %v", err)
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
