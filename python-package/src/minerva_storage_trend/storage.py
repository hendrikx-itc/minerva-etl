# -*- coding: utf-8 -*-
"""
Provides PostgreSQL specific storage functionality using arrays.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing
from functools import partial
import StringIO
import logging
from itertools import chain
from operator import itemgetter
import re
import time

import psycopg2.errorcodes

from minerva.util import first, no_op
from minerva.db.util import enquote_column_name
from minerva.db.error import NoSuchTable
from minerva.db.generic import UniqueViolation
from minerva.db.query import Sql, Table, And, Eq, Column, FromItem, \
        Argument, Literal, LtEq, Gt, ands, Select, WithQuery, As, Value
from minerva.db.postgresql import column_exists
from minerva.directory.helpers import NoSuchEntityError, \
    get_entity, create_entity
from minerva.storage.generic import format_value, \
    RecoverableError, NonRecoverableError, \
    create_full_table_name, extract_data_types, MaxRetriesError, \
    get_data_types, datatype

from tables import create_trend_table, \
    check_columns_exist, create_temp_table_from, add_missing_columns, \
    check_column_types

from minerva_storage_trend.helpers import get_previous_timestamp, \
    get_table_names_v4


SCHEMA = "trend"
LARGE_BATCH_THRESHOLD = 10
MAX_RETRIES = 10


class DataTypeMismatch(Exception):
    pass


class NoSuchColumnError(Exception):
    pass

DATATYPE_MISMATCH_ERRORS = set((
    psycopg2.errorcodes.DATATYPE_MISMATCH,
    psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE,
    psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION))


def refine_data_rows(conn, raw_data_rows):
    return [(get_or_create_entity(conn, dn).id, refine_values(values))
        for dn, values in raw_data_rows]


def get_or_create_entity(conn, dn):
    try:
        entity = get_entity(conn, dn)
    except NoSuchEntityError:
        entity = create_entity(conn, dn)

    return entity


def refine_values(raw_values):
    return [refine_value(value) for value in raw_values]


def refine_value(value):
    if len(value) == 0:
        return None
    elif type(value) is tuple:
        return ",".join(value)
    else:
        return value


def retrieve_aggregated(conn, datasource, granularity, entitytype,
    column_identifiers, interval, group_by, subquery_filter=None,
    relation_table_name=None):
    """
    Return aggregated data

    :param conn: psycopg2 database connection
    :param datasource: datasource object
    :param granularity: granularity in seconds
    :param entitytype: entitytype object
    :param column_identifiers: e.g. SUM(trend1), MAX(trend2)
    :param interval: (start, end) tuple with non-naive timestamps
    :param group_by: list of columns to GROUP BY
    :param subquery_filter: optional subquery for additional filtering
        by JOINing on field 'id' = entity_id
    :param relation_table_name: optional relation table name for converting
            entity ids to related ones
    """
    start, end = interval

    with closing(conn.cursor()) as cursor:
        source_table_names = get_table_names_v4(cursor, [datasource], granularity,
                entitytype, start, end)

    def get_trend_names(column_identifier):
        if isinstance(column_identifier, Sql):
            return [a.name for a in column_identifier.args]
        else:
            trend_names_part = re.match(".*\(([\w, ]+)\)", column_identifier).group(1)

            return map(str.strip, trend_names_part.split(","))

    trend_names = set(chain(*map(get_trend_names, column_identifiers)))

    #Deal with 'samples' column
    if column_exists(conn, SCHEMA, source_table_names[-1], "samples"):
        select_samples_part = "SUM(samples)"
        select_samples_column = "samples,"
    else:
        select_samples_part = "COUNT(*)"
        select_samples_column = ""

    args = {"start": start, "end": end}

    select_parts = []

    for source_table_name in source_table_names:

        join_parts = []

        return_id_field = "entity_id"

        if subquery_filter:
            join_parts.append(
                "JOIN ({0}) AS filter ON filter.id = \"{1}\".{2}.entity_id".format(
                subquery_filter, SCHEMA, enquote_column_name(source_table_name)))

        if relation_table_name:
            return_id_field = "r.target_id AS entity_id"

            join_parts.append(
                "JOIN relation.\"{0}\" r ON r.source_id = \"{1}\".entity_id".format(
                relation_table_name, source_table_name))

        select_parts.append(
            "SELECT {0}, %(end)s, {1} {2} FROM \"{3}\".\"{4}\" {5}"
            " WHERE timestamp > %(start)s AND timestamp <= %(end)s".format(
                return_id_field,
                select_samples_column,
                ",".join(map(enquote_column_name, trend_names)),
                SCHEMA,
                source_table_name,
                " ".join(join_parts)))

    query = ("SELECT entity_id, %(end)s, {0}, {1} FROM( {2} ) "
        "AS sources GROUP BY {3}").format(
            select_samples_part,
            ",".join(map(quote_ident, column_identifiers)),
            " UNION ALL ".join(select_parts),
            ",".join(map(enquote_column_name, group_by)))

    all_rows = []

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query, args)
        except psycopg2.ProgrammingError:
            logging.debug(cursor.mogrify(query, args))
            conn.rollback()
            # TODO: Check error code
        else:
            all_rows = cursor.fetchall()

    return all_rows


def retrieve_orderedby_time(conn, schema, table_names, columns, entities,
        start, end, limit=None):

    all_rows = retrieve(conn, schema, table_names, columns, entities, start,
        end, limit=None)

    all_rows.sort(key=itemgetter(1))

    return all_rows


def retrieve_by_trendids(conn, schema, trends, entities, start, end,
        limit=None):
    """
    Retrieve data.

    :param conn: Minerva database connection
    :param schema Base schema for Trend data
    :param trends: A list of trend ids
    :param entities: List of entity Ids
    :param start: The start timestamp of the range of trend values
    :param end: The end timestamp of the range of trend values
    """
    query = (
        "SELECT trend.name, partition.table_name "
        "FROM trend.trend trend "
        "JOIN trend.trend_partition_link link on link.trend_id = trend.id "
        "JOIN trend.partition partition on "
            "link.partition_table_name = partition.table_name "
        "WHERE trend.id in (%s) AND "
            "%s < partition.data_end AND "
            "%s > partition.data_start")

    all_rows = []
    tables = {}

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(query, (",".join(map(str, trends)), start, end))
        except psycopg2.ProgrammingError:
            conn.rollback()
            # TODO: Check error code
        else:
            rows = cursor.fetchall()
            if rows is not None:
                for trendname, tablename in rows:
                    if tablename not in tables:
                        tables[tablename] = []
                    tables[tablename].append(trendname)

    for table_name, trend_names in tables.items():
        all_rows.extend(retrieve(conn, schema, [table_name], trend_names,
            entities, start, end, limit))

    return all_rows


def quote_ident(ident):
    if isinstance(ident, str):
        return enquote_column_name(ident)
    elif hasattr(ident, "__iter__"):
        return ".".join('"{}"'.format(part) for part in ident)
    elif isinstance(ident, Sql):
        return ident.render()
    else:
        raise Exception("invalid identifier '{}'".format(ident))


def retrieve(conn, schema, table_names, columns, entities, start, end,
        subquery_filter=None, relation_table_name=None, limit=None, entitytype=None):
    """
    Retrieve data.

    :param conn: Minerva database connection
    :param datasource: A DataSource object
    :param granularity_period: The granularity period in seconds
    :param columns: A list of column identifiers (possibly for different
        datasources)
    :param entities: List of entity Ids
    :param start: The start timestamp of the range of trend values
    :param end: The end timestamp of the range of trend values
    :param subquery_filter: optional subquery for additional filtering
        by JOINing on field 'id' = entity_id
    :param relation_table_name: optional relation table name for converting entity
        ids to related ones
    """
    all_rows = []

    if entities is not None and len(entities) == 0:
        return []

    # group tables by partition size signature to be able to JOIN them later
    tables_by_partition_signature = {}
    for table_name in table_names:
        signature = table_name.split("_")[-1]

        tables_by_partition_signature.setdefault(signature, []).append(table_name)

    for table_names in tables_by_partition_signature.values():
        params = []

        if start == end and start is not None and len(table_names) > 1:
            cols = [
                As(Argument(), "timestamp"),
                Column("dn"),
                As(Column("id"), "entity_id")]

            q = Select(cols, from_=Table("directory", "entity"),
                    where_=Eq(Column("entitytype_id"), Value(entitytype.id)))

            with_query = WithQuery("t", query=q)
            params.append(start)

            base_timestamp_column = Column("t", "timestamp")
            base_entity_id_column = Column("t", "entity_id")

            from_item = FromItem(Table("t"))
            data_table_names = table_names
        else:
            with_query = None

            base_tbl = Table(schema, first(table_names))

            base_timestamp_column = Column(base_tbl, "timestamp")
            base_entity_id_column = Column(base_tbl, "entity_id")

            from_item = FromItem(base_tbl)
            data_table_names = table_names[1:]

        for table_name in data_table_names:
            tbl = Table(schema, table_name)

            timestamp_comparison = Eq(Column(tbl, "timestamp"), base_timestamp_column)
            entity_id_comparison = Eq(Column(tbl, "entity_id"), base_entity_id_column)
            join_condition = And(timestamp_comparison, entity_id_comparison)

            from_item = from_item.join(tbl, on=join_condition, join_type="LEFT")

        if subquery_filter:
            filter_tbl = Literal("({0}) AS filter".format(subquery_filter))
            from_item = from_item.join(filter_tbl,
                    on=Eq(Column("filter", "id"), base_entity_id_column))

        if relation_table_name:
            relation_table = Table("relation", relation_table_name)

            join_condition = Eq(Column("r", "source_id"), base_entity_id_column)

            from_item = from_item.join(As(relation_table, "r"), on=join_condition)

            entity_id_column = Column("r", "target_id")
        else:
            entity_id_column = base_entity_id_column

        partition_columns = [entity_id_column, base_timestamp_column] + \
                map(Literal, map(quote_ident, columns))

        where_parts = []

        if not with_query:
            if start == end and start is not None:
                condition = Eq(base_timestamp_column, Argument())
                where_parts.append(condition)
                params.append(start)
            else:
                if not start is None:
                    condition = Gt(base_timestamp_column, Argument())
                    where_parts.append(condition)
                    params.append(start)

                if not end is None:
                    condition = LtEq(base_timestamp_column, Argument())
                    where_parts.append(condition)
                    params.append(end)

        if not entities is None:
            condition = Literal("{0} IN ({1:s})".format(base_entity_id_column.render(),
                ",".join(str(entity_id) for entity_id in entities)))
            where_parts.append(condition)

        if where_parts:
            where_clause = ands(where_parts)
        else:
            where_clause = None

        select = Select(partition_columns, with_query=with_query, from_=from_item,
                where_=where_clause, limit=limit)

        query = select.render()

        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(query, params)
            except psycopg2.ProgrammingError:
                logging.debug(cursor.mogrify(query, params))
                conn.rollback()
                # TODO: Check error code
            else:
                all_rows.extend(cursor.fetchall())

    return all_rows


def retrieve_related(conn, schema, relation_table_name, table_names,
    trend_names, start, end, subquery_filter=None, limit=None):
    """
    Retrieve data for entities of another entity type of trend data. Related
    entities are found via relation table.

    Example: retrieve utrancell data for a set of cell entities (very useful
        in GIS)

    :param conn: Minerva database connection
    :param datasource: A DataSource object
    :param granularity_period: The granularity period in seconds
    :param trend_names: A list of trend names (possibly for different datasources)
    :param start: The start timestamp of the range of trend values
    :param end: The end timestamp of the range of trend values
    :param subquery_filter: optional subquery for additional filtering
        by JOINing on field 'id' = entity_id
    :param limit: Optional limit
    """
    where_parts = []
    all_rows = []

    #group tables by partition size signature to be able to JOIN them later
    tables_by_partition_signature = {}
    for table_name in table_names:
        signature = table_name.split("_")[-1]
        tables_by_partition_signature.setdefault(signature, []).append(table_name)

    for table_names in tables_by_partition_signature.values():
        full_base_tbl_name = "{0}.\"{1}\"".format(schema, table_names[0])

        trend_columns_part = ", ".join(map(enquote_column_name, trend_names))

        query = (
            "SELECT r.source_id, r.target_id, {1}.\"timestamp\", {0} "
            "FROM {1} ").format(trend_columns_part, full_base_tbl_name)

        join_parts = []
        for table_name in table_names[1:]:
            full_tbl_name = "{0}.\"{1}\"".format(schema, table_name)
            join_parts.append(
                " JOIN {0} ON {0}.\"timestamp\" = {1}.\"timestamp\" AND "
                "{0}.entity_id = {1}.entity_id ".format(
                    full_tbl_name, full_base_tbl_name))

        join_parts.append(
            " JOIN relation.\"{0}\" r ON r.target_id = {1}.entity_id ".format(
                relation_table_name, full_base_tbl_name))

        if subquery_filter:
            join_parts.append(
                " JOIN ({0}) AS filter ON filter.id = r.source_id ".format(
                    subquery_filter))

        query += "".join(join_parts)

        where_parts = []

        params = []
        if start == end and start is not None:
            where_parts.append("{0}.\"timestamp\" = %s".format(full_base_tbl_name))
            params = [start]
        else:
            if not start is None:
                where_parts.append("{0}.\"timestamp\" > %s".format(full_base_tbl_name))
                params.append(start)

            if not end is None:
                where_parts.append("{0}.\"timestamp\" <= %s".format(full_base_tbl_name))
                params.append(end)

        if where_parts:
            query += " WHERE " + " AND ".join(where_parts)

        if not limit is None:
            query += " LIMIT {0:d}".format(limit)

        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(query, params)
            except psycopg2.ProgrammingError:
                logging.debug(cursor.mogrify(query, params))
                conn.rollback()
                # TODO: Check error code
            else:
                all_rows.extend(cursor.fetchall())

    return all_rows


def store(conn, schema, table_name, trend_names, timestamp, data_rows,
        sub_query=None):
    retry = True
    attempt = 0

    store_fn = store_insert

    while retry is True:
        retry = False
        attempt += 1

        if attempt > MAX_RETRIES:
            raise MaxRetriesError("Max retries ({0}) reached".format(MAX_RETRIES))

        with closing(conn.cursor()) as cursor:
            modified = get_timestamp(cursor)

        try:
            # Delete destination data if possible to prevent UPDATEs
            if sub_query:
                if (set(["entity_id", "timestamp", "modified"] + trend_names) ==
                        set(get_column_names(conn, SCHEMA, table_name))):
                    try:
                        delete_by_sub_query(conn, table_name, timestamp, sub_query)
                    except NoSuchTable as exc:
                        data_types = extract_data_types(data_rows)
                        fix = partial(create_trend_table, conn, schema, table_name,
                                trend_names, data_types)
                        raise RecoverableError(str(exc), fix)

            store_fn(conn, schema, table_name, trend_names, timestamp, modified,
                    data_rows)

            with closing(conn.cursor()) as cursor:
                mark_modified(cursor, schema, table_name, timestamp, modified)
        except UniqueViolation:
            try:
                conn.rollback()
            except psycopg2.InterfaceError as exc:
                logging.debug(exc)

            store_fn = store_update

            retry = True
        except RecoverableError as err:
            try:
                conn.rollback()
            except psycopg2.InterfaceError as exc:
                logging.debug(exc)

            logging.debug(str(err))
            err.fix()

            retry = True
        else:
            conn.commit()


def get_timestamp(cursor):
    cursor.execute("SELECT NOW()")

    return first(cursor.fetchone())


def mark_modified(cursor, schema, table_name, timestamp, modified):
    args = table_name, timestamp, modified

    try:
        cursor.callproc("trend.mark_modified", args)
    except Exception as exc:
        raise RecoverableError(str(exc), no_op)


def store_insert(conn, schema, table_name, trend_names, timestamp, modified,
        data_rows):
    try:
        if len(data_rows) <= LARGE_BATCH_THRESHOLD:
            store_batch_insert(conn, schema, table_name, trend_names, timestamp,
                    modified, data_rows)
        else:
            store_copy_from(conn, schema, table_name, trend_names, timestamp,
                    modified, data_rows)
    except DataTypeMismatch as exc:
        data_types = extract_data_types(data_rows)
        fix = partial(check_column_types, conn, schema, table_name, trend_names,
                data_types)
        raise RecoverableError(str(exc), fix)
    except NoSuchTable as exc:
        data_types = extract_data_types(data_rows)
        fix = partial(create_trend_table, conn, schema, table_name, trend_names,
                data_types)
        raise RecoverableError(str(exc), fix)
    except NoSuchColumnError as exc:
        fix = partial(check_columns_exist, conn, schema, table_name, trend_names)
        raise RecoverableError(str(exc), fix)


def store_insert_tmp(conn, tmp_table_name, trend_names, timestamp, modified,
        data_rows):
    """
    Same as store_insert, but for temporary tables
    """
    table_name = tmp_table_name.split("tmp_")[-1]

    try:
        if len(data_rows) <= LARGE_BATCH_THRESHOLD:
            store_batch_insert(conn, None, tmp_table_name, trend_names, timestamp,
                    modified, data_rows)
        else:
            store_copy_from(conn, None, tmp_table_name, trend_names, timestamp,
                    modified, data_rows)
    except DataTypeMismatch as exc:
        data_types = extract_data_types(data_rows)
        fix = partial(check_column_types, conn, SCHEMA, table_name, trend_names,
                data_types)
        raise RecoverableError(str(exc), fix)
    except NoSuchColumnError as exc:
        fix = partial(check_columns_exist, conn, SCHEMA, table_name, trend_names)
        raise RecoverableError(str(exc), fix)


def store_update(conn, schema, table_name, trend_names, timestamp, modified,
        data_rows):
    store_using_tmp(conn, schema, table_name, trend_names, timestamp, modified,
            data_rows)
    store_using_update(conn, schema, table_name, trend_names, timestamp, modified,
            data_rows)


def store_copy_from(conn, schema, table, trend_names, timestamp, modified,
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
        where values is a sequence of values in the same order as `trend_names`.
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
            elif exc.pgcode is None and str(exc).find("no COPY in progress") != -1:
                # Might happen after database connection loss
                raise RecoverableError(str(exc), no_op)
            elif exc.pgcode == psycopg2.errorcodes.DEADLOCK_DETECTED:
                back_off = partial(time.sleep, 5)
                raise RecoverableError(str(exc), back_off)
            else:
                raise NonRecoverableError("{0}, {1!s} in query '{2}'".format(
                    exc.pgcode, exc, copy_from_query))


def create_copy_from_file(timestamp, modified, data_rows):
    copy_from_file = StringIO.StringIO()

    lines = create_copy_from_lines(timestamp, modified, data_rows)

    copy_from_file.writelines(lines)

    copy_from_file.seek(0)

    return copy_from_file


def create_copy_from_lines(timestamp, modified, data_rows):
    return (create_copy_from_line(timestamp, modified, data_row)
            for data_row in data_rows)


def create_copy_from_line(timestamp, modified, data_row):
    entity_id, values = data_row

    trend_value_part = "\t".join(format_value(value) for value in values)

    return u"{0:d}\t'{1!s}'\t'{2!s}'\t{3}\n".format(entity_id,
            timestamp.isoformat(), modified.isoformat(), trend_value_part)


def create_copy_from_query(schema, table, trend_names):
    column_names = ["entity_id", "timestamp", "modified"]
    column_names.extend(trend_names)

    full_table_name = create_full_table_name(schema, table)

    return "COPY {0}({1}) FROM STDIN".format(full_table_name,
        ",".join('"{0}"'.format(column_name) for column_name in column_names))


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

    dest_column_names = ",".join('"{0}"'.format(column_name)
            for column_name in column_names)

    parameters = ", ".join(["%s"] * len(column_names))

    full_table_name = create_full_table_name(schema, table)

    query = "INSERT INTO {0} ({1}) VALUES ({2})".format(
        full_table_name, dest_column_names, parameters)

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


def delete_by_sub_query(conn, table_name, timestamp, entityselection):
    """
    Delete rows from table for a specific timestamp and entity_ids in
    entityselection.
    """
    table = Table(SCHEMA, table_name)

    delete_query = (
        "DELETE FROM {} d USING entity_filter f "
        "WHERE d.timestamp = %s AND f.entity_id = d.entity_id"
    ).format(table.render())

    args = (timestamp,)

    with closing(conn.cursor()) as cursor:
        entityselection.create_temp_table(cursor, "entity_filter")

        logging.debug(cursor.mogrify(delete_query, args))

        try:
            cursor.execute(delete_query, args)
        except psycopg2.DatabaseError as exc:
            if exc.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                raise NoSuchTable()
            else:
                raise exc


def aggregate(conn, schema, source, target, trend_names, timestamp):
    """
    Basic aggregation of trend data

    :param conn: psycopg2 database connection
    :param schema: schema where source and target data is located
    :param source: tuple (datasource, gp, entitytype_name) specifying source
    :param target: tuple (datasource, gp, entitytype_name) specifying target
    :param trend_names: trends to aggregate
    :param timestamp: non-naive timestamp specifying end of interval to aggregate
    """
    target_gp = target[1]
    interval = (get_previous_timestamp(timestamp, target_gp), timestamp)

    (ds, gp, et_name) = source
    source_table_names = get_table_names(
        [ds], gp, et_name, interval[0], interval[1])

    target_table_name = make_table_name(*(target + (timestamp,)))

    if column_exists(conn, schema, source_table_names[-1], "samples"):
        select_samples_part = "SUM(samples)"
        select_samples_column = "samples,"
    else:
        select_samples_part = "COUNT(*)"
        select_samples_column = ""

    select_parts = []

    for source_table_name in source_table_names:
        select_parts.append(
            "SELECT "
            "entity_id, '{1}', {2} {3} "
            "FROM \"{0}\".\"{4}\" "
            "WHERE timestamp > %s AND timestamp <= %s ".format(
                schema,
                timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                select_samples_column,
                ",".join(["\"{0}\"".format(tn) for tn in trend_names]),
                source_table_name))

    query = (
        "INSERT INTO \"{0}\".\"{1}\" (entity_id, timestamp, samples, {2}) "
        "SELECT entity_id, '{4}', {5}, {6} FROM "
        "( {3} ) AS sources "
        "GROUP BY entity_id".format(
            schema,
            target_table_name,
            ",".join(["\"{0}\"".format(tn) for tn in trend_names]),
            " UNION ALL ".join(select_parts),
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            select_samples_part,
            ",".join(["SUM(\"{0}\")".format(tn) for tn in trend_names])))

    retry = True
    attempt = 0

    #Strategy followed in code below is like trend_storage.store() function
    while retry is True:
        retry = False
        attempt += 1

        if attempt > MAX_RETRIES:
            raise MaxRetriesError("Max retries ({0}) reached".format(MAX_RETRIES))
        try:
            with closing(conn.cursor()) as cursor:
                cursor.execute(query, len(source_table_names) * interval)
        except psycopg2.DatabaseError as exc:
            conn.rollback()
            columns = [("samples", "integer")]
            columns.extend(zip(trend_names,
                get_data_types(conn, schema, source_table_names[-1], trend_names)))

            if exc.pgcode == psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE:
                max_values = []
                for source_table_name in source_table_names:
                    query_max_values = (
                        "SELECT {0} FROM "
                        "(SELECT "
                        " {1} "
                        "FROM \"{2}\".\"{3}\" "
                        "WHERE timestamp > %s AND timestamp <= %s "
                        "GROUP BY entity_id) AS sums"
                    ).format(
                            ",".join(["MAX(\"{0}\")".format(tn) for tn in trend_names]),
                            ",".join(["SUM(\"{0}\") AS \"{0}\"".format(tn) for tn in trend_names]),
                            schema,
                            source_table_name)

                    with closing(conn.cursor()) as cursor:
                        cursor.execute(query_max_values, interval)
                        max_values.append(cursor.fetchone())

                data_types = [datatype.extract_from_value(v)
                        for v in map(max, zip(*max_values))]
                check_column_types(conn, schema, target_table_name, trend_names,
                        data_types)

                retry = True
            elif exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                raise NonRecoverableError("{0}, {1!s} in query '{2}'".format(
                    exc.pgcode, exc, query))
                # TODO: remove unique violating record from target
                # retry = True
            elif exc.pgcode == psycopg2.errorcodes.UNDEFINED_COLUMN:
                column_names, data_types = zip(*columns)
                add_missing_columns(conn, schema, target_table_name,
                        zip(column_names, data_types))
                retry = True
            elif exc.pgcode == psycopg2.errorcodes.UNDEFINED_TABLE:
                column_names, data_types = zip(*columns)
                create_trend_table(conn, schema, target_table_name, column_names,
                        data_types)
                retry = True
            else:
                raise NonRecoverableError("{0}, {1!s} in query '{2}'".format(
                    exc.pgcode, exc, query))
        else:
            conn.commit()
