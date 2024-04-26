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
        drop_statement = f"DROP TABLE \"{table_name}\";"
        print(f"Deleting table {table_name}..., {drop_statement}")
        response = requests.post(CLICKHOUSE_URL, data=drop_statement.encode("utf-8"))
        if response.status_code != 200:
            print(f"Failed to delete table {table_name}. Reason: {response.text}")
            continue
        deleted_tables.append(table_name)
    print(f"Deleted ClickHouse tables {', '.join(deleted_tables)}")

def create_initial_schema():

    directory = "/schema"
    print("Create initial schema")
    files = os.listdir(directory)

    files.sort()

    for file in files:
        if not file.endswith(".sql"):
            continue
        filename = os.path.join(directory, file)
        with open(filename, 'r') as f:
            content = f.read()
            print(f"Running SQL script: {filename}", )
            response = requests.post(CLICKHOUSE_URL, data=content.encode("utf-8"))
            if response.status_code != 200:
              print(f"Failed to run SQL script {filename}. Reason: {response.text}")
              os.exit(1)

    print("Initial schema created")


if __name__ == "__main__":
    load_dotenv()
    delete_clickhouse_tables()
    create_initial_schema()