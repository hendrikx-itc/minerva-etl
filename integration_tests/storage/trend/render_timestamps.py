import datetime
from functools import partial

from minerva.directory.basetypes import DataSource
from minerva.storage.trend.tables import make_table_name
from minerva.util.tabulate import render_table

WITH_ACCS = {"auto": min, "min": min, "max": max}

GP_QTR = 900
GP_HR = 3600
GP_DAY = 86400


def create_datasource(timezone):
    return DataSource(
        1, name="DummySource", description="Dummy data source", timezone=timezone
    )


ENTITYTYPE_NAME = "DummyType"
DATASOURCE = create_datasource("Europe/Amsterdam")


def main():
    timestamp = DATASOURCE.tzinfo.localize(datetime.datetime(2008, 12, 3, 1, 0, 0))

    timestamps = [timestamp + i * datetime.timedelta(0, GP_HR) for i in range(7 * 24)]

    column_names = ["timestamp", "table_name"]
    alignments = ["<", "<"]
    widths = ["max", "max"]

    table_names = map(
        partial(make_table_name, DATASOURCE, GP_HR, ENTITYTYPE_NAME), timestamps
    )

    rows = list(zip(timestamps, table_names))

    table = render_table(column_names, alignments, widths, rows)

    for line in table:
        print(line)


if __name__ == "__main__":
    main()
