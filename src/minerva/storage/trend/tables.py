# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import re
import logging
from datetime import datetime, timedelta
from contextlib import closing

import psycopg2

from minerva.db.query import Table
from minerva.db.postgresql import grant, column_exists
from minerva.db.error import translate_postgresql_exception
from minerva.storage import datatype
from minerva.storage.generic import NonRecoverableError, get_data_types


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
    "0:05:00": "5m",
    "0:15:00": "qtr",
    "1:00:00": "hr",
    "12:00:00": "12hr",
    "86400": "day",
    "604800": "wk"
}

TABLE_NAME_REGEX = re.compile("^(.+)_(.+)_(.+)_([0-9]+)$")

EPOCH = datetime(1970, 1, 1, 0, 0, 0)

GRANULARITY_PERIODS = {
    "5m": 300,
    "qtr": 900,
    "hr": 3600,
    "day": 86400,
    "wk": 604800,
    "week": 604800}


GP_MAPPING = {
    "5m": 300,
    "qtr": 900,
    "hr": 3600,
    "12hr": 43200,
    "day": 86400,
    "wk": 86400 * 7,
    "month": "month"}


class InvalidTrendTableName(Exception):
    pass


def offset_hack(ref):
    # Get right offset (backward compatibility)
    if ref.utcoffset().total_seconds() > 0:
        ref += timedelta(1)
    return ref.replace(hour=0, minute=0)


def check_columns_exist(conn, schema, table, column_names):
    with closing(conn.cursor()) as cursor:
        for column_name in column_names:
            create_column(cursor, Table(schema, table), column_name, "smallint")


def add_missing_columns(conn, schema, table_name, columns_to_check):
    """
    Add missing columns in `table_name`.

    :param conn: psycopg2 database connection
    :param schema: name of schema where specified table is located
    :param table_name: name of table that will be updated
    :param colums_to_check: list of tuples (column_name, data_type) specifying
    columns that must be checked and added when missing
    """
    with closing(conn.cursor()) as cursor:
        for (column_name, data_type) in columns_to_check:
            if not column_exists(conn, schema, table_name, column_name):
                create_column(
                    cursor, Table(schema, table_name), column_name, data_type
                )


def create_trend_table(conn, schema, table, column_names, data_types):
    """
    :param conn: psycopg2 database connection
    :param schema: name of the database schema to create the table in
    :param table: name of table to be created, or Table instance
    """
    columns_part = "".join(
        "\"{0}\" {1}, ".format(name, data_type)
        for (name, data_type) in zip(column_names, data_types)
    )

    if isinstance(table, str):
        table = Table(schema, table)

    query = (
        "CREATE TABLE {0} ( "
        "entity_id integer NOT NULL, "
        '"timestamp" timestamp with time zone NOT NULL, '
        '"modified" timestamp with time zone NOT NULL, '
        "{1}"
        'CONSTRAINT "{2}_pkey" PRIMARY KEY (entity_id, "timestamp"))'
    ).format(table.render(), columns_part, table.name)

    alter_query = (
        "ALTER TABLE {0} ALTER COLUMN modified "
        "SET DEFAULT CURRENT_TIMESTAMP".format(table.render())
    )

    index_query_modified = (
        'CREATE INDEX "idx_{0}_modified" ON {1} '
        'USING btree (modified)'.format(table.name, table.render())
    )

    index_query_timestamp = (
        'CREATE INDEX "idx_{0}_timestamp" ON {1} '
        'USING btree (timestamp)'.format(table.name, table.render())
    )

    owner_query = "ALTER TABLE {} OWNER TO minerva_writer".format(
        table.render()
    )

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query)
            cursor.execute(alter_query)
            cursor.execute(index_query_modified)
            cursor.execute(index_query_timestamp)
            cursor.execute(owner_query)
        except psycopg2.IntegrityError as exc:
            # apparently the table has been created already, so ignore
            pass
        except psycopg2.ProgrammingError as exc:
            if exc.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
                # apparently the table has been created already, so ignore
                pass
            else:
                raise NonRecoverableError(
                    "ProgrammingError({0}): {1}".format(exc.pgcode, exc.pgerror)
                )
        else:
            grant(conn, "TABLE", "SELECT", table.render(), "minerva")
            grant(conn, "TABLE", "TRIGGER", table.render(), "minerva_writer")
            conn.commit()


def create_temp_table_from(conn, schema, table):
    """
    Create a temporary table that inherits from `table` and return the temporary
    table name.
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


def check_column_types(conn, schema, table, column_names, data_types):
    """
    Check if database column types match trend datatype and correct it if
    necessary.
    """
    current_data_types = get_data_types(conn, schema, table, column_names)

    with closing(conn.cursor()) as cursor:
        for column_name, current_data_type, data_type in zip(
                column_names, current_data_types, data_types):
            required_data_type = datatype.max_datatype(
                current_data_type, data_type
            )

            if required_data_type != current_data_type:
                logging.debug(
                    "{} != {}".format(required_data_type, current_data_type)
                )

                args = table, column_name, required_data_type

                cursor.callproc("trend.modify_partition_column", args)

                logging.info(
                    "Column {0:s} modified from type {1} to {2}".format(
                        column_name, current_data_type, required_data_type))

    conn.commit()


def create_column(cursor, table, column_name, data_type):
    """
    Create a new column with matching datatype for the specified trend.
    """
    table_ref = table.render()

    query = (
        "ALTER TABLE {0} "
        "ADD COLUMN \"{1}\" {2}"
    ).format(table_ref, column_name, data_type)

    try:
        cursor.execute(query)
    except psycopg2.ProgrammingError as exc:
        raise translate_postgresql_exception(exc)
