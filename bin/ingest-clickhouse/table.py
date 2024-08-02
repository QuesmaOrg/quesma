import re
import requests
from datetime import datetime, timezone
from tabulate import tabulate

import clickhouse

NESTING_SEPARATOR = '_' # data {"a": {"b": 5}} will become a_b: 5 in Clickhouse

class Table:
    def __init__(self, table_name, urls, step_by_step):
        self.data_jsons = []
        self.table_name = table_name
        self.urls = list(urls)
        self.step_by_step = step_by_step

    @staticmethod
    def download_jsons_from_one_url(url):
        data = requests.get(url)
        return data.json()["expected"]

    @staticmethod
    def flatten_json(json, nested_path=None):
        if nested_path is None:
            nested_path = []
        flat_json = {}
        for key, value in json.items():
            if isinstance(value, dict):
                flat_json.update(Table.flatten_json(value, nested_path + [key]))
            else:
                flat_json[NESTING_SEPARATOR.join(nested_path + [key])] = value
        return flat_json

    @staticmethod
    def escape_quotes_in_string_like_clickhouse_wants(json):
        for key, value in json.items():
            if isinstance(value, str):
                json[key] = value.replace('\"', '\\"')
                json[key] = json[key].replace(r"\'", "\\'")
        return json

    @staticmethod
    def sort_fields(fields):
        fields_sorted = []
        for field in fields:
            first_separator = field.name.find(NESTING_SEPARATOR)
            if first_separator == -1:
                fields_sorted.append(field)
                continue
            first_part = field.name[:first_separator]
            last_matched_idx = len(fields_sorted)
            for idx, f in enumerate(fields_sorted):
                if len(f.name) >= first_separator and f.name[:first_separator] == first_part:
                    last_matched_idx = idx
            fields_sorted.insert(last_matched_idx, field)

        return fields_sorted

    # find example values in data for all fields (returns <= 3 different examples)
    def find_examples(self, field: clickhouse.Field):
        max_examples_nr = 3
        for js in self.data_jsons:
            if field.name in js and js[field.name] not in field.examples:
                field.examples.append(js[field.name])
                if len(field.examples) == max_examples_nr:
                    break

    def download_data(self):
        for url in self.urls:
            self.data_jsons += self.download_jsons_from_one_url(url)

    def preprocess_data(self):
        pass

    def transform_data_to_clickhouse_format(self):
        self.data_jsons = [self.flatten_json(json) for json in self.data_jsons]
        self.data_jsons = [self.escape_quotes_in_string_like_clickhouse_wants(json) for json in self.data_jsons]

    def postprocess_data(self):
        for i in range(len(self.data_jsons)):
            # merge "date" and "time" fields
            if 'date' in self.data_jsons[i] and 'time' in self.data_jsons[i]:
                self.data_jsons[i]['date'] = self.data_jsons[i]['date'] + ' ' + self.data_jsons[i]['time']
                del self.data_jsons[i]['time']

            for field_name in self.data_jsons[i]:
                # improve timestamps
                val = self.data_jsons[i][field_name]
                # we look for format '2022-05-25T13:25:26.000Z' or '2022-05-25T13:25:26Z'
                regex_no_timezone = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}((\.\d{3}Z)|Z)$"
                if isinstance(val, str) and re.search(regex_no_timezone, val) is not None:
                    timestamp_good_format = val[:10] + ' ' + val[11:-1] # T->space, Z->empty
                    self.data_jsons[i][field_name] = timestamp_good_format

                # or '2022-05-25T13:25:26.000+02:00'
                regex_timezone = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?[\+-]\d{2}:\d{2}$"
                if isinstance(val, str) and re.search(regex_timezone, val) is not None:
                    timestamp_good_format = val[:10] + ' ' + val[11:]
                    dt = datetime.fromisoformat(timestamp_good_format)
                    dt_utc = dt.astimezone(timezone.utc)
                    new_timestamp_almost_good = dt_utc.isoformat(timespec='milliseconds')[:-6] # -6 removes +00:00
                    new_timestamp = new_timestamp_almost_good.replace('T', ' ')
                    self.data_jsons[i][field_name] = new_timestamp

    def do_all_work(self):
        # download and process data
        self.download_data()
        self.preprocess_data()
        self.transform_data_to_clickhouse_format()
        self.postprocess_data()

        # create table
        fields = clickhouse.get_create_table_description(self.table_name, self.data_jsons)
        fields = self.sort_fields(fields)
        for field in fields:
            self.find_examples(field)
        if self.step_by_step:
            print(tabulate(fields, headers=['Field', 'Type', 'Examples']))

        if self.step_by_step:
            if not ask_user("Do you want to create the table " + self.table_name + "? [y/n]"):
                return
        clickhouse.send_create_table_query(self.table_name, fields, print_query=self.step_by_step)

        if self.step_by_step:
            if not ask_user("Do you want to add " + str(len(self.data_jsons)) + " rows to the table? [y/n]"):
                return
        clickhouse.send_ingest(self.table_name, self.data_jsons)


def ask_user(question):
    print(question)
    answer = input()
    if answer == 'y' or answer == 'Y':
        return True
    elif answer == 'n' or answer == 'N':
        return False
    else:
        return ask_user(question)
