package clickhouse

import (
	"crypto/tls"
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"
	"mitmproxy/quesma/buildinfo"
	"mitmproxy/quesma/quesma/config"
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
	options.ClientInfo.Products = append(options.ClientInfo.Products, info)

	return clickhouse.OpenDB(&options)
}
