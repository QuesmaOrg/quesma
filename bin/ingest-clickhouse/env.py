from table_data import TableData

CLICKHOUSE_ADDRESS = 'localhost'
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = ''

# change to your needs
EXAMPLE = TableData(
    table_name='example-table',
    urls=('https://raw.githubusercontent.com/something1/something2.log',)
)

TABLES = [
    EXAMPLE
]

