#!/usr/bin/env python3
import os
import requests
from dotenv import load_dotenv


LOG_FILE_PREFIX = "/var/mitmproxy/requests/"
QUERY_FILE_PREFIX = "/var/mitmproxy/query/"
PLACEHOLDER=".placeholder"
CLICKHOUSE_URL="http://clickhouse:8123/"

def _delete_files_in_dir(dir_path):
    if not os.path.isdir(dir_path):
        print(f"Error: {dir_path} is not a directory.")
        return
    
    log_deleted = []

    for filename in os.listdir(dir_path): 
        file_path = os.path.join(dir_path, filename)
        if filename != PLACEHOLDER and os.path.isfile(file_path):
            try:
                os.remove(file_path)
                log_deleted.append(filename)
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")

    if len(log_deleted) > 0:
        logs = ",".join(log_deleted)
        if len(log_deleted) > 1:
            logs = '{' + logs + '}'
        print(f"Deleted: {dir_path}{logs}")

def delete_log_files():
    _delete_files_in_dir(LOG_FILE_PREFIX)
    _delete_files_in_dir(QUERY_FILE_PREFIX)

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



if __name__ == "__main__":
    load_dotenv()
    delete_log_files()
    delete_clickhouse_tables()