#!/usr/bin/env python3
import argparse

import clickhouse
import env
import table

DEFAULT_STEP_BY_STEP = True # by default, we don't run the entire script at one, but step by step

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='Drop Tables Script',
        description='Drops tables from variable TABLES in env.py',
    )
    parser.add_argument('-f', '--force', default=DEFAULT_STEP_BY_STEP,
                        action='store_false', help='Run the entire script at once, not step-by-step')
    args = parser.parse_args()
    step_by_step = args.force

    we_drop = True
    print('About to drop tables:', *(f'\n* {t.table_name}' for t in env.TABLES))
    if step_by_step:
        if not table.ask_user("\nAre you sure you want to drop all those tables? [y/n]\nIf only some, "
                              "please edit env.py file to include only the ones you want to drop in TABLES variable."):
            we_drop = False

    dropped = []
    not_dropped = []
    if we_drop:
        for t in env.TABLES:
            if clickhouse.send_drop_table(t.table_name):
                dropped.append(t.table_name)
            else:
                not_dropped.append(t.table_name)

    if dropped:
        print("\nDropped tables:", *(f'\n* {d}' for d in dropped))
    if not_dropped:
        print("\nNot dropped tables (probably not-existing):", *(f'\n* {nd}' for nd in not_dropped))
