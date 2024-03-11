package clickhouse

import (
	"crypto/tls"
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"
	"mitmproxy/quesma/quesma/config"
)

func InitDBConnectionPool(c config.QuesmaConfiguration) *sql.DB {
	options := clickhouse.Options{Addr: []string{c.ClickHouseUrl.Host}}
	if c.ClickHouseUser != "" || c.ClickHousePassword != "" || c.ClickHouseDatabase != "" {
		options.TLS = &tls.Config{
			InsecureSkipVerify: true, // TODO: fix it
		}

		options.Auth = clickhouse.Auth{
			Username: c.ClickHouseUser,
			Password: c.ClickHousePassword,
			Database: c.ClickHouseDatabase,
		}
	}

	return clickhouse.OpenDB(&options)
}
