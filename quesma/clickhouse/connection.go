package clickhouse

import (
	"crypto/tls"
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"
	"mitmproxy/quesma/quesma/config"
)

func initDBConnectionPool(configuration config.QuesmaConfiguration) *sql.DB {
	options := clickhouse.Options{Addr: []string{configuration.ClickHouseUrl.Host}}
	if configuration.ClickHouseUser != nil || configuration.ClickHousePassword != nil || configuration.ClickHouseDatabase != nil {
		options.TLS = &tls.Config{
			InsecureSkipVerify: true, // TODO: fix it
		}

		options.Auth = clickhouse.Auth{
			Username: withDefault(configuration.ClickHouseUser, ""),
			Password: withDefault(configuration.ClickHousePassword, ""),
			Database: withDefault(configuration.ClickHouseDatabase, ""),
		}
	}

	return clickhouse.OpenDB(&options)
}
