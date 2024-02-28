package ui

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqlPrettyPrint(t *testing.T) {
	sql := `SELECT * FROM "logs-generic-default" WHERE (message LIKE '%user%' AND (timestamp>=parseDateTime64BestEffort('2024-01-22T09:26:10.299Z') AND timestamp<=parseDateTime64BestEffort('2024-12-22T09:41:10.299Z')))`

	sqlFormatted := sqlPrettyPrint([]byte(sql))

	sqlExpected := `SELECT *
FROM "logs-generic-default"
WHERE message LIKE '%user%'
AND (
    "timestamp" >= parsedatetime64besteffort('2024-01-22T09:26:10.299Z')
    AND "timestamp" <= parsedatetime64besteffort('2024-12-22T09:41:10.299Z')
  );`

	assert.Equal(t, sqlExpected, sqlFormatted)
}

func TestSqlPrettyPrint_multipleSqls(t *testing.T) {
	sql := `SELECT '', '', count() FROM "kibana_sample_data_ecommerce" WHERE (("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR ("order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))) AND ("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z'))
SELECT '', '', count() FROM "kibana_sample_data_ecommerce" WHERE (("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR ("order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))) AND ("order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z'))
SELECT '', '', '', sum("taxful_total_price") FROM "kibana_sample_data_ecommerce" WHERE ("order_date">=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date"<=parseDateTime64BestEffort('2024-02-26T12:59:40.626Z')) OR ("order_date"<=parseDateTime64BestEffort('2024-02-19T12:59:40.626Z') AND "order_date">=parseDateTime64BestEffort('2024-02-12T12:59:40.626Z'))`

	sqlFormatted := sqlPrettyPrint([]byte(sql))
	sqlExpected := `SELECT '',
'',
count()
FROM kibana_sample_data_ecommerce
WHERE (
  (
    order_date >= parsedatetime64besteffort('2024-02-19T12:59:40.626Z')
    AND order_date <= parsedatetime64besteffort('2024-02-26T12:59:40.626Z')
  )
  OR (
      order_date <= parsedatetime64besteffort('2024-02-19T12:59:40.626Z')
      AND order_date >= parsedatetime64besteffort('2024-02-12T12:59:40.626Z')
    )
)
AND (
    order_date >= parsedatetime64besteffort('2024-02-19T12:59:40.626Z')
    AND order_date <= parsedatetime64besteffort('2024-02-26T12:59:40.626Z')
  );

SELECT '',
'',
count()
FROM kibana_sample_data_ecommerce
WHERE (
  (
    order_date >= parsedatetime64besteffort('2024-02-19T12:59:40.626Z')
    AND order_date <= parsedatetime64besteffort('2024-02-26T12:59:40.626Z')
  )
  OR (
      order_date <= parsedatetime64besteffort('2024-02-19T12:59:40.626Z')
      AND order_date >= parsedatetime64besteffort('2024-02-12T12:59:40.626Z')
    )
)
AND (
    order_date >= parsedatetime64besteffort('2024-02-12T12:59:40.626Z')
    AND order_date <= parsedatetime64besteffort('2024-02-19T12:59:40.626Z')
  );

SELECT '',
'',
'',
sum(taxful_total_price)
FROM kibana_sample_data_ecommerce
WHERE (
  order_date >= parsedatetime64besteffort('2024-02-19T12:59:40.626Z')
  AND order_date <= parsedatetime64besteffort('2024-02-26T12:59:40.626Z')
)
OR (
    order_date <= parsedatetime64besteffort('2024-02-19T12:59:40.626Z')
    AND order_date >= parsedatetime64besteffort('2024-02-12T12:59:40.626Z')
  );`

	assert.Equal(t, sqlExpected, sqlFormatted)
}

// Test checking if SqlPrettyPrint deals well with backticks.
// If you don't process backticks accordingly, SqlPrettyPrint throws an error.
func TestSqlPrettPrintBackticks(t *testing.T) {
	sql := "SELECT toInt64(toUnixTimestamp64Milli(`@timestamp`)/30000), count() FROM " + `"logs-generic-default" WHERE ("@timestamp">=parseDateTime64BestEffort('2024-02-04T11:11:29.735Z') AND "@timestamp"<=parseDateTime64BestEffort('2024-02-04T11:26:29.735Z')) AND ("@timestamp">=timestamp_sub(SECOND,900, now64())) GROUP BY toInt64(toUnixTimestamp64Milli(` + "`@timestamp`)/30000)"
	sqlFormatted := sqlPrettyPrint([]byte(sql))
	assert.Greater(t, len(strings.Split(sqlFormatted, "\n")), 1) // if error, SqlPrettyPrint returns input string with len == 1
}

func TestInvalidSql(t *testing.T) {
	invalidSql := `this sql is a joke, you are not going to parse it`
	sqlFormatted := sqlPrettyPrint([]byte(invalidSql))
	assert.Equal(t, invalidSql, sqlFormatted)
}
