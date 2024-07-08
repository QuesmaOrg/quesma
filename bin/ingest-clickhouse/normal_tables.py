from table import Table
import env

NORMAL_TABLES = [table for table in env.TABLES] # if table != env.EXAMPLE_SPECIAL_TABLE]
# Remove/filter out some tables from here, if you add special ones.


class NormalTable(Table):
    def __init__(self, table_data, step_by_step):
        super().__init__(table_data.table_name, table_data.urls, step_by_step)
