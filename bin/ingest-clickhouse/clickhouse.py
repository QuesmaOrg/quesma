import clickhouse_connect
import json

from collections import namedtuple

import env

Field = namedtuple("field", ["name", "type", "examples"])

client = clickhouse_connect.get_client(host=env.CLICKHOUSE_ADDRESS, username=env.CLICKHOUSE_USER,
                                       password=env.CLICKHOUSE_PASSWORD)


def send_query_to_clickhouse(query):
    return client.command(query)


def get_create_table_description(table_name, jsons):
    jsons_as_string = ','.join([json.dumps(j) for j in jsons])
    query = f"DESC format(JSONEachRow, `{jsons_as_string}`)"
    ch_result = send_query_to_clickhouse(query)

    fields = []
    i = 0
    while i < len(ch_result):
        col = ch_result[i].strip("\n")

        if col == "":
            i += 1
            continue

        fields += [Field(col, ch_result[i + 1], [])]
        i += 2  # we read [i] (name) and [i+1] (type) so we need to skip the next one

    return fields


# returns True/False if we created or not
def send_create_table_query(table_name, fields, print_query):
    query = f'CREATE TABLE "{table_name}" (\n'
    max_len = max([len(field.name) for field in fields]) + 1
    for field in fields:
        query += f'\t"{field.name}"' + ' ' * (max_len - len(field.name)) + f'{field.type},\n'
    query = query[:-2] + "\n) ENGINE=Log"
    if print_query:
        print(query)
    try:
        send_query_to_clickhouse(query)
        print("Table " + table_name + " created.")
        return True
    except:
        print("Table " + table_name + " wasn't created, probably it already exists. Drop it first if you want to "
                                      "recreate it.")
        return False


def send_ingest(table_name, data):
    data_as_string = '\n'.join([json.dumps(j) for j in data])
    query = f'INSERT INTO "{table_name}" FORMAT JSONEachRow\n{data_as_string}'
    send_query_to_clickhouse(query)
    print("Added " + str(len(data)) + " rows.") # works for our data, check it better for the future


def send_drop_table(table_name):
    query = f'DROP TABLE "{table_name}"'
    try:
        send_query_to_clickhouse(query)
        return True
    except:
        return False
