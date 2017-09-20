# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import logging
from contextlib import closing

import psycopg2

from minerva.storage import datatype


def NoOpFix():
    pass


class RecoverableError(Exception):
    def __init__(self, msg, fix):
        Exception.__init__(self, msg)
        self.fix = fix


class NonRecoverableError(Exception):
    pass


class MaxRetriesError(Exception):
    pass


def create_full_table_name(schema, table):
    if schema is not None:
        return "\"{0}\".\"{1}\"".format(schema, table)
    else:
        return "\"{0}\"".format(table)


def format_value(value):
    """
    Format a parsed value for use in a stream accepted by the COPY FROM command
    in PostgreSQL.
    """
    if value is None:
        return u"\\N"
    elif isinstance(value, tuple) or isinstance(value, list):
        return "{{{}}}".format(
            ",".join('"{}"'.format(format_value(part)) for part in value)
        )
    else:
        return value


def escape_value(value):
    if value == u"\\N":
        return value
    else:
        return value.replace(
            '\\', '\\\\'
        ).replace(
            '\r', '\\r'
        ).replace(
            '\n', '\\n'
        )


def create_column(conn, schema, table, column_name, data_type):
    """
    Create a new column with matching datatype for the specified trend.
    """
    full_table_name = create_full_table_name(schema, table)

    query = "ALTER TABLE {0} ADD COLUMN \"{1}\" {2};".format(
        full_table_name, column_name, data_type)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query)
        except psycopg2.ProgrammingError as exc:
            conn.rollback()
            # The column might have been created somewhere else
            logging.debug(str(exc))
        else:
            conn.commit()


def extract_data_types(data_rows):
    data_types = None

    for _entity_id, values in data_rows:
        row_data_types = [
            datatype.parser_descriptor_from_string(value).data_type
            for value in values
        ]

        if data_types is None:
            data_types = row_data_types
        else:
            data_types = datatype.max_data_types(data_types, row_data_types)

    return data_types


def get_data_types(conn, schema, table, column_names):
    data_types = []

    query = (
        "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type "
        "FROM pg_class c, pg_attribute a, pg_type t, pg_namespace n "
        "WHERE c.relname = %s "
        "AND n.nspname = %s "
        "AND a.attname = %s "
        "AND a.attrelid = c.oid "
        "AND a.atttypid = t.oid "
        "AND c.relnamespace = n.oid")

    with closing(conn.cursor()) as cursor:
        for column_name in column_names:
            args = (table, schema, column_name)

            cursor.execute(query, args)

            if cursor.rowcount > 0:
                (data_type, ) = cursor.fetchone()

                data_types.append(data_type)
            else:
                raise NonRecoverableError("No such column: {0}.{1}".format(
                    table, column_name))

    return data_types


def check_column_types(conn, schema, table, column_names, data_types):
    """
    Check if database column types match trend datatype and correct it if
    necessary.
    """
    current_data_types = get_data_types(conn, schema, table, column_names)
    full_table_name = create_full_table_name(schema, table)

    with closing(conn.cursor()) as cursor:
        for column_name, current_data_type, data_type in zip(
                column_names, current_data_types, data_types):
            required_data_type = datatype.max_datatype(
                current_data_type, data_type)

            if required_data_type != current_data_type:
                logging.debug("{} != {}".format(required_data_type,
                                                current_data_type))

                query = (
                    "ALTER TABLE {0} "
                    "ALTER \"{1}\" TYPE {2} "
                    "USING CAST(\"{1}\" AS {2})").format(
                    full_table_name, column_name, required_data_type)

                cursor.execute(query)

                logging.info(
                    "Column {0:s} modified from type {1} to {2}".format(
                        column_name, current_data_type, required_data_type))
    conn.commit()
