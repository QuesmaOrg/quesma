For script to work, you need to change first 3 variables in `env.py` with your Clickhouse instance values.

Afterwards, creating tables in ClickHouse with automatic insert of data is as simple as that:
```bash
pip3 install -r requirements.txt
python3 full_insert_script.py -f
```

If you want to see how it works (with much more info logs), you can run the script without `-f`, so
```bash
python3 full_insert_script.py
```
You'll have to type `y` a few times to make the script proceed.

You can play with it how much you want, because there's also `drop_tables.py` script, which will
drop all tables created by `full_insert_script.py` script. After that you can run `full_insert_script.py` again.
It also has `-f` flag so you can run it via either: `(step-by-step: on)`
```bash
python3 drop_tables.py
```
or `force/step-by-step: off`
```bash
python3 drop_tables.py -f
```

In `env.py` there's also `TABLES` variable. You can comment out/delete some lines from it, then
both scripts will operate only on remaining tables.

If you want to add your own tables (table name + url with data), you can add them to `TABLES` variable in `env.py`.
There's an example of how to do it in `env.py` file.
If table data is in different format than "normal", you also need to add handling to `special_tables.py`, remove the table
from `NORMAL_TABLES` in `normal_tables.py`, and add it to `SPECIAL_TABLES` in `special_tables.py`.
