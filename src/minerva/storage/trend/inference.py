# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"
from minerva.util import first

from minerva.storage.trend.schema import system_columns_set


def get_tables(cursor, schema):
    query = (
        "SELECT relname, "
        "CASE relkind "
        "WHEN 'r' THEN 'table'::trend.storetype "
        "WHEN 'v' THEN 'view'::trend.storetype "
        "ELSE NULL "
        "END "
        "FROM pg_class "
        "JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid "
        "WHERE nspname = 'trend' AND (relkind = 'r' OR relkind = 'v')"
    )

    args = tuple()

    cursor.execute(query, args)

    return cursor.fetchall()


def get_column_names(cursor, partition_name):
    query = (
        "SELECT attname FROM pg_attribute "
        "JOIN pg_class ON pg_class.oid = pg_attribute.attrelid "
        "WHERE relname = %s AND attnum > 0 AND attisdropped = FALSE"
    )

    args = (partition_name,)

    cursor.execute(query, args)

    return map(first, cursor.fetchall())


def is_trend_name(name):
    return name not in system_columns_set


def get_trend_names(cursor, partition_name):
    return filter(is_trend_name, get_column_names(cursor, partition_name))
