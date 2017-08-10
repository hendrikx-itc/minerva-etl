# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from contextlib import closing

from minerva.db.query import Table


PARTITION_SIZES = {
    "300": 86400,
    "900": 86400,
    "1800": 86400,
    "3600": 7 * 86400,
    "43200": 30 * 86400,
    "86400": 30 * 86400,
    "604800": 210 * 86400,
    "month": 365 * 86400}

# Table name postfixes by interval size
DATA_TABLE_POSTFIXES = {
    "00:05:00": "5m",
    "00:15:00": "qtr",
    "01:00:00": "hr",
    "12:00:00": "12hr",
    "86400": "day",
    "1 day": "day",
    "604800": "wk"
}

EPOCH = datetime(1970, 1, 1, 0, 0, 0)


def offset_hack(ref):
    # Get right offset (backward compatibility)
    if ref.utcoffset().total_seconds() > 0:
        ref += timedelta(1)
    return ref.replace(hour=0, minute=0)


def create_temp_table_from(conn, schema, table):
    """
    Create a temporary table that inherits from `table` and
    return the temporary table name.
    """
    if isinstance(table, str):
        table = Table(schema, table)

    tmp_table_name = "tmp_{0}".format(table.name)

    query = (
        "CREATE TEMPORARY TABLE \"{0}\" (LIKE {1}) "
        "ON COMMIT DROP"
    ).format(tmp_table_name, table.render())

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

    return tmp_table_name
