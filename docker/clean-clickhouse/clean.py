#!/usr/bin/env python3
import os
import requests
from dotenv import load_dotenv

PLACEHOLDER=".placeholder"
CLICKHOUSE_URL="http://clickhouse:8123/"

def delete_clickhouse_tables():

    if os.getenv("DROP_TABLES") == "false":
        print("DROP_TABLES is set to false. Skipping deleting Clickhouse tables.")
        return
    response = requests.get(CLICKHOUSE_URL, params={"query": "SHOW TABLES"})
    if response.status_code != 200:
        print(f"Failed to get tables from clickhouse. Reason: {response.text}")
        return
    table_names = [line.strip() for line in response.text.split("\n") if line.strip() != ""]
    
    if len(table_names) == 0:
        print("No tables found to delete in ClickHouse.")
        return
    
    print(f"Tables in ClickHouse about to be deleted: {', '.join(table_names)}")
    # loop through all tables and delete them
    deleted_tables = []
    for table_name in table_names:

        # HACK this is our table created on database initialization
        # see docker/clickhouse/docker-entrypoint-initdb.d/device_logs.sql
        #
        if table_name == "device_logs":
            continue

        drop_statement = f"DROP TABLE \"{table_name}\";"
        print(f"Deleting table {table_name}..., {drop_statement}")
        response = requests.post(CLICKHOUSE_URL, data=drop_statement.encode("utf-8"))
        if response.status_code != 200:
            print(f"Failed to delete table {table_name}. Reason: {response.text}")
            continue
        deleted_tables.append(table_name)
    print(f"Deleted ClickHouse tables {', '.join(deleted_tables)}")

if __name__ == "__main__":
    load_dotenv()
    delete_clickhouse_tables()