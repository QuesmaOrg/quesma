#!/usr/bin/env python3
import argparse
from typing import List, Tuple

import env
from normal_tables import NormalTable, NORMAL_TABLES
from special_tables import SPECIAL_TABLES
from table import Table

DEFAULT_STEP_BY_STEP = True  # by default, we don't run the entire script at one, but step by step


# returns 2 lists: (recognized_tables, unrecognized_tables)
def get_tables_to_work_on(step_by_step: bool) -> Tuple[List[Table], List[str]]:
    tables, unrecognized_tables = [], []
    for table in env.TABLES:
        if table in NORMAL_TABLES:
            tables.append(NormalTable(table, step_by_step))
        elif table in SPECIAL_TABLES:
            tables.append(SPECIAL_TABLES[table](table, step_by_step))
        else:
            unrecognized_tables.append(table.table_name)

    return tables, unrecognized_tables


def main():
    parser = argparse.ArgumentParser(
        prog='Full Insert Script',
        description='Creates tables from env.py/TABLES if they don\'t exist + ingests sample data',
    )
    parser.add_argument('-f', '--force', default=DEFAULT_STEP_BY_STEP, action='store_false',
                        help='Run the entire script at once, not step-by-step')
    args = parser.parse_args()
    step_by_step = args.force

    tables, unrecognized_tables = get_tables_to_work_on(step_by_step)
    for table in tables:
        table.do_all_work()

    print("Unrecognized tables: ", unrecognized_tables)


if __name__ == '__main__':
    main()
