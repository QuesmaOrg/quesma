from typing import Dict, Type

import env
from table import Table
from table_data import TableData


# change to your needs, adding/reimplementing methods from Table that you need
class ExampleSpecialTable(Table):
    def __init__(self, table_data, step_by_step):
        super().__init__(table_data.table_name, table_data.urls, step_by_step)


SPECIAL_TABLES: Dict[TableData, Type[Table]] = {
    # env.EXAMPLE_SPECIAL_TABLE: ExampleSpecialTable # Add some tables here, if you add special ones.
}
