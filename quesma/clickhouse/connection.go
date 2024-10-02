// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0
package clickhouse

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"net"
	"quesma/buildinfo"
	"quesma/logger"
	"quesma/quesma/config"
	"strings"
	"time"
)

func initDBConnection(c *config.QuesmaConfiguration, tlsConfig *tls.Config) *sql.DB {

	options := clickhouse.Options{Addr: []string{c.ClickHouse.Url.Host}}
	if c.ClickHouse.User != "" || c.ClickHouse.Password != "" || c.ClickHouse.Database != "" {

		options.Auth = clickhouse.Auth{
			Username: c.ClickHouse.User,
			Password: c.ClickHouse.Password,
			Database: c.ClickHouse.Database,
		}
	}
	if !c.ClickHouse.DisableTLS {
		options.TLS = tlsConfig
	}

	info := struct {
		Name    string
		Version string
	}{
		Name:    "quesma",
		Version: buildinfo.Version,
	}

	// Setting limit here is not working. It causes runtime error.
	// Set it after opening the connection.
	//
	//	options.MaxIdleConns = 50
	//	options.MaxOpenConns = 50
	//	options.ConnMaxLifetime = 0

	options.ClientInfo.Products = append(options.ClientInfo.Products, info)

	return clickhouse.OpenDB(&options)

}

func InitDBConnectionPool(c *config.QuesmaConfiguration) *sql.DB {
	if c.ClickHouse.Url == nil {
		return nil
	}

	db := initDBConnection(c, &tls.Config{})

	err := db.Ping()
	if err != nil {

		// These error message duplicates messages from end_user_errors.GuessClickhouseErrorType
		// Not sure if you want to keep them in sync or not. These two cases are different.

		if strings.Contains(err.Error(), "tls: failed to verify certificate") {
			logger.Warn().Err(err).Msg("Failed to connect to database with TLS. Retrying TLS, but with disabled chain and host verification.")
			_ = db.Close()
			db = initDBConnection(c, &tls.Config{InsecureSkipVerify: true})
		} else if strings.Contains(err.Error(), "tls: first record does not look like a TLS handshake") {
			_ = db.Close()
			logger.Warn().Err(err).Msg("Failed to connect to database with TLS. Trying without TLS at all.")
			db = initDBConnection(c, nil)
		}
	}

	err = db.Ping()
	if err != nil {
		logger.Error().Err(err).Msg("Failed to connect to database. There can be errors in further requests.")
		if !c.TransparentProxy {
			RunClickHouseConnectionDoctor(c)
		}
		// Other errors are not handled here, eg. authentication error, database not found, etc.
		// Maybe we should return the error here and Quesma should handle it.
	} else {
		logger.Info().Msg("Connected to database: " + c.ClickHouse.Url.String())
	}

	// The default is pretty low. We need to increase it.
	// FIXME this should set in the configuration
	db.SetMaxIdleConns(20) // default is 5
	db.SetMaxOpenConns(30) // default is 10
	// clean up connections after 5 minutes, before that they may be killed by the firewall
	db.SetConnMaxLifetime(time.Duration(5) * time.Minute) // default is 1h

	return db
}

// RunClickHouseConnectionDoctor is very blunt and verbose function which aims to print some helpful information
// in case of misconfigured ClickHouse connection. In the future, we might rethink how do we manage this and perhaps
// move some parts to InitDBConnectionPool, but for now this should already provide some useful feedback.
func RunClickHouseConnectionDoctor(c *config.QuesmaConfiguration) {
	timeout := 1 * time.Second
	defaultNativeProtocolPort := "9000"
	defaultNativeProtocolPortEncrypted := "9440"

	logger.Info().Msgf("[connection-doctor] Starting ClickHouse connection doctor")
	hostName, port := c.ClickHouse.Url.Hostname(), c.ClickHouse.Url.Port()
	address := fmt.Sprintf("%s:%s", hostName, port)
	logger.Info().Msgf("[connection-doctor] Trying to dial configured host/port (%s)...", address)
	connTcp, errTcp := net.DialTimeout("tcp", address, timeout)
	if errTcp != nil {
		logger.Info().Msgf("[connection-doctor] Failed dialing with %s, err=[%v], no service listening at configured host/port, make sure to specify reachable ClickHouse address", address, errTcp)
		logger.Info().Msgf("[connection-doctor] Trying default ClickHouse native ports...")
		if conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", hostName, defaultNativeProtocolPort), timeout); err == nil {
			logger.Info().Msgf("[connection-doctor] Default ClickHouse plaintext port is reachable, consider changing ClickHouse port to %s in Quesma configuration", defaultNativeProtocolPort)
			conn.Close()
		}
		if conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", hostName, defaultNativeProtocolPortEncrypted), timeout); err == nil {
			logger.Info().Msgf("[connection-doctor] Default ClickHouse TLS port is reachable, consider changing ClickHouse port to %s in Quesma conbfiguration", defaultNativeProtocolPortEncrypted)
			conn.Close()
		}
		return
	}
	defer connTcp.Close()
	logger.Info().Msgf("[connection-doctor] Trying to establish TLS connection with configured host/port (%s)", address)
	connTls, errTls := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", address, &tls.Config{InsecureSkipVerify: true})
	if errTls != nil {
		logger.Info().Msgf("[connection-doctor] Failed establishing TLS connection with %s, err=[%v], please use `clickhouse.disableTLS: true` in Quesma configuration", address, errTls)
		return
	}
	defer connTls.Close()
	logger.Info().Msgf("[connection-doctor] TLS connection (handshake) with %s established successfully", address)
}
