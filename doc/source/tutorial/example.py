# -*- coding: utf-8 -*-
from contextlib import closing

from minerva.db import connect
from minerva.directory import add_datasource
from minerva.data import store_data

DB_URI = "postgresql://example:password@localhost/minerva"
GRANULARITY_PERIOD = 900
PARTITION_SIZE = 86400
ENTITYTYPE_NAME = "UtranCell"
DATASOURCE_NAME = "pm"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
TIMESTAMP = "2010-02-15 13:00:00"


def main():
    with closing(connect(DB_URI)) as conn:
        datasource = add_datasource(conn, "example", "", "Europe/Amsterdam", "trend")


if __name__ == "__main__":
    main()
