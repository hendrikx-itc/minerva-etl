# -*- coding: utf-8 -*-
"""
Provides PostgreSQL specific storage functionality using arrays.
"""
from contextlib import closing
from functools import partial
from io import StringIO
import logging
from itertools import chain
from operator import itemgetter
import re
import time

import psycopg2.errorcodes

from minerva.util import first, no_op
from minerva.db.util import quote_ident
from minerva.db.error import NoSuchTable
from minerva.db.generic import UniqueViolation
from minerva.db.query import Sql, Table, And, Eq, Column, FromItem, \
    Argument, Literal, LtEq, Gt, ands, Select, WithQuery, As, Value
from minerva.db.postgresql import column_exists
from minerva.directory.entity import Entity
from minerva.storage.generic import format_value, \
    RecoverableError, NonRecoverableError, \
    create_full_table_name, extract_data_types, MaxRetriesError, \
    get_data_types, datatype
from minerva.storage.trend.tables import create_temp_table_from
from minerva.storage.trend.helpers import get_previous_timestamp, \
    get_table_names_v4


SCHEMA = "trend"
LARGE_BATCH_THRESHOLD = 10
MAX_RETRIES = 10


class DataTypeMismatch(Exception):
    pass


class NoSuchColumnError(Exception):
    pass


DATATYPE_MISMATCH_ERRORS = {
    psycopg2.errorcodes.DATATYPE_MISMATCH,
    psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE,
    psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION
}


def refine_data_rows(conn, raw_data_rows):
    with closing(conn.cursor()) as cursor:
        return [
            (Entity.from_dn(dn)(cursor).id, refine_values(values))
            for dn, values in raw_data_rows
        ]


def refine_values(raw_values):
    return [refine_value(value) for value in raw_values]


def refine_value(value):
    if len(value) == 0:
        return None
    elif type(value) is tuple:
        return ",".join(value)
    else:
        return value


def get_timestamp(cursor):
    cursor.execute("SELECT NOW()")

    return first(cursor.fetchone())


def mark_modified(cursor, schema, table_name, timestamp, modified):
    args = table_name, timestamp, modified

    try:
        cursor.callproc("trend.mark_modified", args)
    except Exception as exc:
        raise RecoverableError(str(exc), no_op)


def store_insert(
        conn, schema, table_name, trend_names, timestamp, modified,
        data_rows):
    if len(data_rows) <= LARGE_BATCH_THRESHOLD:
        store_batch_insert(
            conn, schema, table_name, trend_names, timestamp,
            modified, data_rows
        )
    else:
        store_copy_from(
            conn, schema, table_name, trend_names, timestamp,
            modified, data_rows
        )


def store_insert_tmp(
        conn, tmp_table_name, trend_names, timestamp, modified,
        data_rows):
    """
    Same as store_insert, but for temporary tables
    """
    if len(data_rows) <= LARGE_BATCH_THRESHOLD:
        store_batch_insert(
            conn, None, tmp_table_name, trend_names, timestamp, modified,
            data_rows
        )
    else:
        store_copy_from(
            conn, None, tmp_table_name, trend_names, timestamp, modified,
            data_rows
        )


def store_update(conn, schema, table_name, trend_names, timestamp, modified,
        data_rows):
    store_using_tmp(conn, schema, table_name, trend_names, timestamp, modified,
            data_rows)
    store_using_update(conn, schema, table_name, trend_names, timestamp, modified,
            data_rows)


def store_copy_from(
        conn, schema, table, trend_names, timestamp, modified,
        data_rows):
    """
    Store the data using the PostgreSQL specific COPY FROM command

    :param conn: DBAPI2 database connection
    :param schema: Name of Minerva data schema
    :param table: Name of table, including schema
    :param timestamp: Timestamp of the data.
    :param trend_names: The trend names in the same order as the values in \
        `data_rows`.
    :param data_rows: A sequence of 2-tuples like (entity_id, values), \
        where values is a sequence of values
        in the same order as `trend_names`.
    """
    copy_from_file = create_copy_from_file(timestamp, modified, data_rows)

    copy_from_query = create_copy_from_query(schema, table, trend_names)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.copy_expert(copy_from_query, copy_from_file)
        except psycopg2.DatabaseError as exc:
            if exc.pgcode in DATATYPE_MISMATCH_ERRORS:
                raise DataTypeMismatch(str(exc))
            elif exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                raise UniqueViolation()
            elif exc.pgcode == psycopg2.errorcodes.UNDEFINED_COLUMN:
                raise NoSuchColumnError()
            elif exc.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                raise NoSuchTable()
            elif exc.pgcode is None and str(exc).find(
                    "no COPY in progress") != -1:
                # Might happen after database connection loss
                raise RecoverableError(str(exc), no_op)
            elif exc.pgcode == psycopg2.errorcodes.DEADLOCK_DETECTED:
                back_off = partial(time.sleep, 5)
                raise RecoverableError(str(exc), back_off)
            else:
                raise NonRecoverableError("{0}, {1!s} in query '{2}'".format(
                    exc.pgcode, exc, copy_from_query))


def create_copy_from_file(timestamp, modified, data_rows):
    copy_from_file = StringIO()

    lines = create_copy_from_lines(timestamp, modified, data_rows)

    copy_from_file.writelines(lines)

    copy_from_file.seek(0)

    return copy_from_file


def create_copy_from_lines(timestamp, modified, data_rows):
    return (
        create_copy_from_line(timestamp, modified, data_row)
        for data_row in data_rows
    )


def create_copy_from_line(timestamp, modified, data_row):
    entity_id, values = data_row

    trend_value_part = "\t".join(format_value(value) for value in values)

    return u"{0:d}\t'{1!s}'\t'{2!s}'\t{3}\n".format(
        entity_id,timestamp.isoformat(), modified.isoformat(),
        trend_value_part)


def create_copy_from_query(schema, table, trend_names):
    column_names = ["entity_id", "timestamp", "modified"]
    column_names.extend(trend_names)

    full_table_name = create_full_table_name(schema, table)

    return "COPY {0}({1}) FROM STDIN".format(
        full_table_name,
        ",".join(
            '"{0}"'.format(column_name)
            for column_name in column_names
        )
    )


def store_using_update(conn, schema, table, trend_names, timestamp, modified,
        data_rows):
    set_columns = ", ".join("\"{0}\"=%s".format(name) for name in trend_names)

    full_table_name = create_full_table_name(schema, table)

    update_query = (
        'UPDATE {0} SET modified=greatest(modified, %s), {1} '
        'WHERE entity_id=%s AND "timestamp"=%s').format(full_table_name, set_columns)

    rows = [list(chain((modified,), values, (entity_id, timestamp)))
            for entity_id, values in data_rows]

    with closing(conn.cursor()) as cursor:
        try:
            cursor.executemany(update_query, rows)
        except psycopg2.DatabaseError as exc:
            if exc.pgcode == psycopg2.errorcodes.UNDEFINED_COLUMN:
                fix = partial(check_columns_exist, conn, schema, table, trend_names)

                raise RecoverableError(str(exc), fix)
            else:
                raise NonRecoverableError(str(exc))


def store_using_tmp(conn, schema, table, trend_names, timestamp, modified,
        data_rows):
    """
    Store the data using the PostgreSQL specific COPY FROM command and a
    temporary table. The temporary table is joined against the target table
    to make sure only new records are inserted.
    """
    tmp_table_name = create_temp_table_from(conn, schema, table)

    store_insert_tmp(conn, tmp_table_name, trend_names, timestamp, modified,
            data_rows)

    column_names = ['entity_id', 'timestamp']
    column_names.extend(trend_names)

    tmp_column_names = ",".join('tmp."{0}"'.format(name) for name in column_names)
    dest_column_names = ",".join('"{0}"'.format(name) for name in column_names)

    full_table_name = create_full_table_name(schema, table)

    insert_query = " ".join([
        "INSERT INTO {0} ({1})".format(full_table_name, dest_column_names),
        "SELECT {0} FROM \"{1}\" AS tmp".format(tmp_column_names, tmp_table_name),
        "LEFT JOIN {0} ON tmp.\"timestamp\" = {0}.\"timestamp\" "
            "AND tmp.entity_id = {0}.entity_id".format(full_table_name),
        "WHERE \"{0}\".entity_id IS NULL".format(table)])

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(insert_query)
        except psycopg2.Error as exc:
            if exc.pgcode == psycopg2.errorcodes.UNDEFINED_COLUMN:
                fix = partial(check_columns_exist, conn, schema, table, trend_names)
                raise RecoverableError(str(exc), fix)
            elif exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                conn.rollback()
                store_insert_rows(conn, schema, table, trend_names, timestamp, modified,
                        data_rows)
            else:
                raise NonRecoverableError(str(exc))


def store_insert_rows(conn, schema, table, trend_names, timestamp, modified,
        data_rows):
    column_names = ["entity_id", "timestamp", "modified"]
    column_names.extend(trend_names)
    columns = ",".join('"{0}"'.format(column_name)
            for column_name in column_names)
    data_placeholders = ", ".join(["%s"] * len(column_names))

    full_table_name = create_full_table_name(schema, table)

    select_query = (
        "SELECT 1 FROM {0} "
        "WHERE entity_id = %s AND timestamp = %s"
    ).format(full_table_name)

    insert_query = "INSERT INTO {0} ({1}) VALUES ({2})".	format(
        full_table_name, columns, data_placeholders)

    with closing(conn.cursor()) as cursor:
        for entity_id, values in data_rows:
            data = [entity_id, timestamp, modified]
            data.extend(values)
            cursor.execute(select_query, [entity_id, timestamp])
            if cursor.rowcount == 0:
                cursor.execute(insert_query, data)


def store_batch_insert(conn, schema, table, trend_names, timestamp, modified,
        data_rows):
    column_names = ["entity_id", "timestamp", "modified"]
    column_names.extend(trend_names)

    dest_column_names = ",".join(
        '"{0}"'.format(column_name)
        for column_name in column_names
    )

    parameters = ", ".join(["%s"] * len(column_names))

    full_table_name = create_full_table_name(schema, table)

    query = "INSERT INTO {0} ({1}) VALUES ({2})".format(
        full_table_name, dest_column_names, parameters
    )

    rows = []

    for entity_id, values in data_rows:
        row = [entity_id, timestamp, modified]
        row.extend(values)

        rows.append(row)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.executemany(query, rows)
        except psycopg2.DatabaseError as exc:
            if exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                raise UniqueViolation()
            elif exc.pgcode == psycopg2.errorcodes.UNDEFINED_COLUMN:
                raise NoSuchColumnError()
            elif exc.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                raise NoSuchTable()
            elif exc.pgcode in DATATYPE_MISMATCH_ERRORS:
                raise DataTypeMismatch(str(exc))
            else:
                raise NonRecoverableError("{0}: {1}".format(exc.pgcode, exc))
