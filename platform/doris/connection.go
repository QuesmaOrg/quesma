// Copyright Quesma, licensed under the Elastic License 2.0.
// SPDX-License-Identifier: Elastic-2.0

package doris

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"github.com/QuesmaOrg/quesma/platform/backend_connectors"
	"github.com/QuesmaOrg/quesma/platform/config"
	"github.com/QuesmaOrg/quesma/platform/logger"
	quesma_api "github.com/QuesmaOrg/quesma/platform/v2/core"
	"strings"
	"time"
)

func InitDBConnectionPool(c *config.QuesmaConfiguration) quesma_api.BackendConnector {
	if c.ClickHouse.Url == nil {
		return nil
	}

	db := initDBConnection(c, &tls.Config{})

	err := db.Ping()
	if err != nil {
		// These error message duplicates messages from end_user_errors.GuessClickhouseErrorType
		// Not sure if you want to keep them in sync or not. These two cases are different.

		if strings.Contains(err.Error(), "tls: failed to verify certificate") {
			logger.Info().Err(err).Msg("Failed to connect to database with TLS. Retrying TLS, but with disabled chain and host verification.")
			_ = db.Close()
			db = initDBConnection(c, &tls.Config{InsecureSkipVerify: true})
		} else if strings.Contains(err.Error(), "tls: first record does not look like a TLS handshake") {
			_ = db.Close()
			logger.Info().Err(err).Msg("Failed to connect to database with TLS. Trying without TLS at all.")
			db = initDBConnection(c, nil)
		} else {
			logger.Info().Err(err).Msg("Failed to ping database and could not apply recovery.")
		}
	}

	// The default is pretty low. We need to increase it.
	// FIXME this should set in the configuration
	db.SetMaxIdleConns(20) // default is 5
	db.SetMaxOpenConns(30) // default is 10
	// clean up connections after 5 minutes, before that they may be killed by the firewall
	db.SetConnMaxLifetime(time.Duration(5) * time.Minute) // default is 1h

	return backend_connectors.NewDorisConnectorWithConnection(c.ClickHouse.Url.String(), db)
}

func initDBConnection(c *config.QuesmaConfiguration, tlsConfig *tls.Config) *sql.DB {

	//todo Clickhouse needs to be changed to a universal connection configuration
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true",
		c.ClickHouse.User,
		c.ClickHouse.Password,
		c.ClickHouse.Url.Host,
		c.ClickHouse.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logger.Error().Err(err).Msg("failed to initialize Doris connection pool")
		return nil
	}

	if err := db.Ping(); err != nil {
		logger.Error().Err(err).Msg("failed to ping Doris server")
		return nil
	}

	logger.Info().Msg("Doris connection pool initialized successfully")
	return db

}
