package clickhouse

import (
	"crypto/tls"
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"
	"mitmproxy/quesma/buildinfo"
	"mitmproxy/quesma/quesma/config"
	"time"
)

func InitDBConnectionPool(c config.QuesmaConfiguration) *sql.DB {
	options := clickhouse.Options{Addr: []string{c.ClickHouse.Url.Host}}
	if c.ClickHouse.User != "" || c.ClickHouse.Password != "" || c.ClickHouse.Database != "" {
		options.TLS = &tls.Config{
			InsecureSkipVerify: true, // TODO: fix it
		}

		options.Auth = clickhouse.Auth{
			Username: c.ClickHouse.User,
			Password: c.ClickHouse.Password,
			Database: c.ClickHouse.Database,
		}
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

	db := clickhouse.OpenDB(&options)

	// The default is pretty low. We need to increase it.

	// FIXME this should set in the configuration
	db.SetMaxIdleConns(20) // default is 5
	db.SetMaxOpenConns(30) // default is 10
	// clean up connections after 5 minutes, before that they may be killed by the firewall
	db.SetConnMaxLifetime(time.Duration(5) * time.Minute) // default is 1h

	return db
}
