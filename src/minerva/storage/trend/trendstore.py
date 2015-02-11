# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import logging
from functools import partial
from operator import methodcaller
import StringIO
from itertools import chain

import psycopg2

from minerva.util import first, no_op
from minerva.db.error import NoCopyInProgress, NoSuchTable, \
    NoSuchColumnError, UniqueViolation, DataTypeMismatch, DuplicateTable, \
    translate_postgresql_exception, translate_postgresql_exceptions
from minerva.db.query import Table, Column, Eq, column_exists, ands
from minerva.db.postgresql import table_exists
from minerva.directory import DataSource, EntityType
from minerva.directory.helpers_v4 import dns_to_entity_ids
from minerva.storage.generic import datatype, format_value, escape_value
from minerva.db.dbtransaction import DbTransaction, DbAction, \
    insert_before, replace, drop_action
from minerva.storage.trend.tables import DATA_TABLE_POSTFIXES
from minerva.storage.trend import schema
from minerva.storage.trend.datapackage import DataPackage
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.tables import create_column
from minerva.storage.trend.partition import Partition
from minerva.storage.trend.partitioning import Partitioning
from minerva.storage.trend.trend import Trend, TrendDescriptor

LARGE_BATCH_THRESHOLD = 10


class TrendStoreDescriptor(object):
    def __init__(
            self, datasource, entitytype, granularity, trend_descriptors,
            partition_size):
        self.datasource = datasource
        self.entitytype = entitytype
        self.granularity = granularity
        self.trend_descriptors = trend_descriptors
        self.partition_size = partition_size


class TrendStore(object):
    """
    All data belonging to a specific datasource, entitytype and granularity.
    """
    def __init__(
            self, id, datasource, entitytype, granularity, partition_size,
            type, trends):
        self.id = id
        self.datasource = datasource
        self.entitytype = entitytype
        self.granularity = granularity
        self.partition_size = partition_size
        self.type = type
        self.partitioning = Partitioning(partition_size)
        self.trends = trends

    def __str__(self):
        return self.base_table_name()

    def base_table_name(self):
        granularity_name = DATA_TABLE_POSTFIXES.get(
            str(self.granularity), str(self.granularity)
        )

        return "{}_{}_{}".format(
            self.datasource.name, self.entitytype.name, granularity_name
        )

    def partition_table_name(self, timestamp):
        base_table_name = self.base_table_name()

        if self.type == "view":
            return base_table_name
        else:
            index = self.partitioning.index(timestamp)

            return "{}_{}".format(base_table_name, index)

    def base_table(self):
        return Table("trend", self.base_table_name())

    def partition(self, timestamp):
        index = self.partitioning.index(timestamp)

        return Partition(index, self)

    def index_to_interval(self, partition_index):
        return self.partitioning.index_to_interval(partition_index)

    def check_trends_exist(self, trend_descriptors):
        def f(cursor):
            base_table = self.base_table()

            for trend_descriptor in trend_descriptors:
                if not column_exists(cursor, base_table, trend_descriptor.name):
                    create_column(
                        cursor, base_table, trend_descriptor.name,
                        trend_descriptor.data_type
                    )

                    assure_trendstore_trend(
                        cursor, self, trend_descriptor.name,
                        trend_descriptor.data_type
                    )

        return f

    def check_column_types(self, trend_descriptors):
        """
        Check if database column types match trend datatype and correct it if
        necessary.
        """
        column_names = [t.name for t in trend_descriptors]
        data_types = [t.data_type for t in trend_descriptors]

        def f(cursor):
            table = self.base_table()
            current_data_types = get_data_types(cursor, table, column_names)

            changes = []

            for column_name, current_data_type, data_type in \
                    zip(column_names, current_data_types, data_types):
                required_data_type = datatype.max_datatype(
                    current_data_type, data_type
                )

                if required_data_type != current_data_type:
                    changes.append((column_name, required_data_type))

                    logging.info(
                        "Column {0:s} requires change from type "
                        "{1} to {2}".format(
                            column_name, current_data_type, required_data_type)
                    )

            query = (
                "SELECT trend_directory.modify_trendstore_columns("
                "%s, "
                "%s::trend_directory.column_info[]"
                ")"
            )

            args = self.id, changes

            cursor.execute(query, args)

        return f

    def table_names(self, start, end):
        timestamps = self.granularity.range(start, end)

        table_names = map(self.partition_table_name, timestamps)

        # HACK for dealing with intervals that are small but span two tables
        # (e.g. 2012-1-5 0:00 - 2012-1-5 1:00 for qtr tables)
        end_table = self.partition_table_name(end)

        table_names.append(end_table)

        return list(set(table_names))

    def tables(self, start, end):
        make_table = partial(Table, "trend")
        table_names = self.table_names(start, end)

        return map(make_table, table_names)

    def get_trend(self, cursor, trend_name):
        query = (
            "SELECT id, name, data_type, trendstore_id, description "
            "FROM trend_directory.trend "
            "WHERE trendstore_id = %s AND name = %s"
        )

        args = self.id, trend_name

        cursor.execute(query, args)

        if cursor.rowcount > 0:
            return Trend(*cursor.fetchone())

    @staticmethod
    def get_trends(cursor, trendstore_id):
        query = (
            "SELECT id, name, data_type, trendstore_id, description "
            "FROM trend_directory.trend "
            "WHERE trendstore_id = %s"
        )

        args = (trendstore_id, )

        cursor.execute(query, args)

        return [Trend(*row) for row in cursor.fetchall()]

    @staticmethod
    def create(descriptor):
        def f(cursor):
            args = (
                descriptor.datasource.name,
                descriptor.entitytype.name,
                str(descriptor.granularity),
                [
                    (d.name, d.data_type, d.description)
                    for d in descriptor.trend_descriptors
                ],
                descriptor.partition_size
            )

            query = (
                "SELECT * FROM trend_directory.create_trendstore("
                "%s, %s, %s, %s::trend_directory.trend_descr[], %s"
                ")"
            )

            cursor.execute(query, args)

            (
                trendstore_id, entitytype_id, datasource_id, granularity_str,
                partition_size, type, retention_period
            ) = cursor.fetchone()

            entitytype = EntityType.get(entitytype_id)(cursor)
            datasource = DataSource.get(datasource_id)(cursor)

            trends = TrendStore.get_trends(cursor, trendstore_id)

            return TrendStore(
                trendstore_id, datasource, entitytype,
                create_granularity(granularity_str), partition_size, type,
                trends
            )

        return f

    def save(self, cursor):
        if self.id is None:
            return self.create(cursor)
        else:
            args = (
                self.datasource.id, self.entitytype.id, self.granularity.name,
                self.partition_size, self.type, self.id
            )

            query = (
                "UPDATE trend_directory.trendstore SET "
                "datasource_id = %s, "
                "entitytype_id = %s, "
                "granularity = %s, "
                "partition_size = %s, "
                "type = %s, "
                "WHERE id = %s"
            )

            cursor.execute(query, args)

            return self

    column_names = [
        "id", "datasource_id", "entitytype_id", "granularity",
        "partition_size", "type"
    ]

    columns = map(Column, column_names)

    get_query = schema.trendstore.select(columns).where_(ands([
        Eq(Column("datasource_id")),
        Eq(Column("entitytype_id")),
        Eq(Column("granularity"))
    ]))

    get_by_id_query = schema.trendstore.select(columns).where_(Eq(Column("id")))

    @classmethod
    def get(cls, cursor, datasource, entitytype, granularity):
        args = datasource.id, entitytype.id, str(granularity)

        cls.get_query.execute(cursor, args)

        if cursor.rowcount > 1:
            raise Exception(
                "more than 1 ({}) trendstore matches".format(cursor.rowcount)
            )
        elif cursor.rowcount == 1:
            (
                trendstore_id, datasource_id, entitytype_id, granularity_str,
                partition_size, type
            ) = cursor.fetchone()

            trends = TrendStore.get_trends(cursor, trendstore_id)

            return TrendStore(
                trendstore_id, datasource, entitytype, granularity,
                partition_size, type, trends
            )

    @classmethod
    def get_by_id(cls, cursor, id):
        args = (id,)

        cls.get_by_id_query.execute(cursor, args)

        if cursor.rowcount == 1:
            (
                trendstore_id, datasource_id, entitytype_id, granularity_str,
                partition_size, type
            ) = cursor.fetchone()

            datasource = DataSource.get(datasource_id)(cursor)
            entitytype = EntityType.get(entitytype_id)(cursor)

            trends = TrendStore.get_trends(cursor, id)

            granularity = create_granularity(granularity_str)

            return TrendStore(
                trendstore_id, datasource, entitytype, granularity,
                partition_size, type, trends
            )

    def has_trend(self, cursor, trend_name):
        query = (
            "SELECT 1 FROM trend_directory.trend "
            "WHERE trendstore_id = %s AND name = %s"
        )

        args = self.id, trend_name

        cursor.execute(query, args)

        return cursor.rowcount > 0

    def store(self, datapackage):
        if datapackage.is_empty():
            return DbTransaction()
        else:
            if len(datapackage.rows) <= LARGE_BATCH_THRESHOLD:
                insert_action = BatchInsert
            else:
                insert_action = CopyFrom

            return DbTransaction(
                StoreState(self, datapackage),
                [
                    SetModified(),
                    insert_action(),
                    MarkModified()
                ]
            )

    def store_raw(self, raw_datapackage):
        if raw_datapackage.is_empty():
            return DbTransaction(None, [])
        else:
            if len(raw_datapackage.rows) <= LARGE_BATCH_THRESHOLD:
                insert_action = BatchInsert
            else:
                insert_action = CopyFrom

            return DbTransaction(
                StoreRawState(self, raw_datapackage),
                [
                    RefineRawDataPackage(),
                    SetModified(),
                    insert_action(),
                    MarkModified()
                ]
            )

    def clear_timestamp(self, timestamp):
        def f(cursor):
            query = (
                "SELECT trend_directory.cleare_timestamp(trendstore, %s) "
                "FROM trend_directory.trendstore "
                "WHERE id = %s"
            )

            args = timestamp, self.id

            cursor.execute(query, args)

        return f


def assure_trendstore_trend(cursor, trendstore, trend_name, data_type):
    if not trendstore.has_trend(cursor, trend_name):
        Trend.create(
            trendstore.id,
            TrendDescriptor(trend_name, data_type, "")
        )(cursor)

        logging.info("created trend {}".format(trend_name))


read = partial(methodcaller, 'get')


class StoreState(object):
    def __init__(self, trendstore, datapackage):
        self.trendstore = trendstore
        self.datapackage = datapackage
        self.modified = None


def trend_descriptors_from_data_package(data_package):
    return [
        TrendDescriptor(name, data_type, 'Created by CopyFrom')
        for name, data_type in zip(
            data_package.trend_names, data_package.deduce_data_types()
        )
    ]


class StoreRawState(object):
    def __init__(self, trendstore, raw_datapackage):
        self.trendstore = trendstore
        self.raw_datapackage = raw_datapackage
        self.datapackage = None
        self.modified = None


class RefineRawDataPackage(DbAction):
    def execute(self, cursor, state):
        try:
            state.datapackage = refine_datapackage(
                cursor, state.raw_datapackage
            )
        except UniqueViolation:
            return no_op


class SetModified(DbAction):
    def execute(self, cursor, state):
        state.modified = get_timestamp(cursor)


class MarkModified(DbAction):
    def execute(self, cursor, state):
        try:
            mark_modified(
                cursor,
                state.trendstore.id,
                state.datapackage.timestamp,
                state.modified
            )
        except UniqueViolation:
            return no_op


class CopyFrom(DbAction):
    def execute(self, cursor, state):
        try:
            store_copy_from(
                cursor,
                state.trendstore.partition(
                    state.datapackage.timestamp
                ).table(),
                state.datapackage,
                state.modified
            )
        except NoCopyInProgress:
            return no_op
        except NoSuchTable:
            return insert_before(CreatePartition())
        except NoSuchColumnError:
            return insert_before(CheckColumnsExist())
        except UniqueViolation:
            return replace(Update())
        except DataTypeMismatch:
            return insert_before(CheckDataTypes())


class BatchInsert(DbAction):
    def execute(self, cursor, state):
        try:
            store_batch_insert(
                cursor,
                state.trendstore.partition(
                    state.datapackage.timestamp
                ).table(),
                state.datapackage,
                state.modified
            )
        except NoSuchTable:
            return insert_before(CreatePartition())
        except NoSuchColumnError:
            return insert_before(CheckColumnsExist())
        except UniqueViolation:
            return replace(Update())
        except DataTypeMismatch:
            return insert_before(CheckDataTypes())


class Update(DbAction):
    def execute(self, cursor, state):
        try:
            store_update(
                cursor,
                state.trendstore.partition(state.datapackage.timestamp).table(),
                state.datapackage,
                state.modified
            )
        except NoSuchTable:
            return insert_before(CreatePartition())
        except NoSuchColumnError:
            return insert_before(CheckColumnsExist())
        except DataTypeMismatch:
            return insert_before(CheckDataTypes())


class CheckColumnsExist(DbAction):
    def execute(self, cursor, state):
        state.trendstore.check_trends_exist(
            trend_descriptors_from_data_package(state.datapackage)
        )(cursor)


class CreatePartition(DbAction):
    def execute(self, cursor, state):
        try:
            state.trendstore.partition(
                state.datapackage.timestamp
            ).create(cursor)

            logging.debug(table_exists(cursor, 'trend_partition', state.trendstore.partition(state.datapackage.timestamp).name()))
        except DuplicateTable:
            return drop_action()


class CheckDataTypes(DbAction):
    def execute(self, cursor, state):
        state.trendstore.check_column_types(
            trend_descriptors_from_data_package(state.datapackage)
        )(cursor)


def get_timestamp(cursor):
    cursor.execute("SELECT NOW()")

    return first(cursor.fetchone())


def store_copy_from(cursor, table, datapackage, modified):
    """
    Store the data using the PostgreSQL specific COPY FROM command

    :param conn: DBAPI2 database connection
    :param table: Name of table, including schema
    :param datapackage: A DataPackage object
    """
    copy_from_file = create_copy_from_file(
        datapackage.timestamp, modified, datapackage.rows
    )

    copy_from_query = create_copy_from_query(table, datapackage.trend_names)

    logging.debug(copy_from_query)

    try:
        try:
            cursor.copy_expert(copy_from_query, copy_from_file)
        except Exception as exc:
            logging.debug(exc)
            raise exc
    except psycopg2.DatabaseError as exc:
        if exc.pgcode is None and str(exc).find("no COPY in progress") != -1:
            # Might happen after database connection loss
            raise NoCopyInProgress()
        else:
            raise translate_postgresql_exception(exc)


def create_copy_from_file(timestamp, modified, data_rows):
    copy_from_file = StringIO.StringIO()

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

    trend_value_part = "\t".join(
        escape_value(format_value(value)) for value in values
    )

    return u"{0:d}\t'{1!s}'\t'{2!s}'\t{3}\n".format(
        entity_id, timestamp.isoformat(), modified.isoformat(),
        trend_value_part
    )


def create_copy_from_query(table, trend_names):
    column_names = chain(schema.system_columns, trend_names)

    quote = partial(str.format, '"{}"')

    query = "COPY {0}({1}) FROM STDIN WITH NULL '\\N'".format(
        table.render(),
        ",".join(map(quote, column_names))
    )

    return query


@translate_postgresql_exceptions
def mark_modified(cursor, trendstore_id, timestamp, modified):
    args = trendstore_id, timestamp, modified

    cursor.callproc("trend_directory.mark_modified", args)


def store_update(cursor, table, datapackage, modified):
    tmp_table = create_temp_table_from(cursor, table)

    store_copy_from(cursor, tmp_table, datapackage, modified)

    # Update existing records
    store_using_update(
        cursor, tmp_table, table, datapackage.trend_names, modified
    )

    # Fill in missing records
    store_using_tmp(cursor, tmp_table, table, datapackage.trend_names)


def store_using_update(cursor, tmp_table, table, column_names, modified):
    set_columns = ", ".join(
        '"{0}"={1}."{0}"'.format(name, tmp_table.render())
        for name in column_names
    )

    update_query = (
        'UPDATE {0} SET modified=greatest(%s, {0}.modified), {1} '
        'FROM {2} '
        'WHERE {0}.entity_id={2}.entity_id AND {0}."timestamp"={2}."timestamp"'
    ).format(table.render(), set_columns, tmp_table.render())

    args = (modified, )

    try:
        cursor.execute(update_query, args)
    except psycopg2.DatabaseError as exc:
        raise translate_postgresql_exception(exc)


def store_using_tmp(cursor, tmp_table, table, column_names):
    """
    Store the data using the PostgreSQL specific COPY FROM command and a
    temporary table. The temporary table is joined against the target table
    to make sure only new records are inserted.
    """
    all_column_names = ['entity_id', 'timestamp']
    all_column_names.extend(column_names)

    tmp_column_names = ", ".join(
        'tmp."{0}"'.format(name)
        for name in all_column_names
    )

    dest_column_names = ", ".join(
        '"{0}"'.format(name)
        for name in all_column_names
    )

    insert_query = (
        "INSERT INTO {table} ({dest_columns}) "
        "SELECT {tmp_columns} FROM {tmp_table} AS tmp "
        "LEFT JOIN {table} ON "
        "tmp.\"timestamp\" = {table}.\"timestamp\" "
        "AND tmp.entity_id = {table}.entity_id "
        "WHERE {table}.entity_id IS NULL"
    ).format(
        table=table.render(),
        dest_columns=dest_column_names,
        tmp_columns=tmp_column_names,
        tmp_table=tmp_table.render()
    )

    try:
        cursor.execute(insert_query)
    except psycopg2.Error as exc:
        raise translate_postgresql_exception(exc)


def store_batch_insert(cursor, table, datapackage, modified):
    column_names = ["entity_id", "timestamp", "modified"]
    column_names.extend(datapackage.trend_names)

    dest_column_names = ",".join(
        '"{0}"'.format(column_name)
        for column_name in column_names
    )

    parameters = ", ".join(["%s"] * len(column_names))

    query = (
        "INSERT INTO {0} ({1}) "
        "VALUES ({2})"
    ).format(table.render(), dest_column_names, parameters)

    rows = [
        (entity_id, datapackage.timestamp, modified) + tuple(values)
        for entity_id, values in datapackage.rows
    ]

    try:
        cursor.executemany(query, rows)
    except psycopg2.DatabaseError as exc:
        logging.debug(cursor.mogrify(query, first(rows)))

        raise translate_postgresql_exception(exc)


def create_temp_table_from(cursor, table):
    """
    Create a temporary table that is like `table` and return the temporary
    table name.
    """
    tmp_table = Table("tmp_{0}".format(table.name))

    query = (
        "CREATE TEMPORARY TABLE {0} (LIKE {1}) "
        "ON COMMIT DROP"
    ).format(tmp_table.render(), table.render())

    cursor.execute(query)

    return tmp_table


def get_column_names(cursor, table):
    """
    Return list of column names of table
    """
    query = (
        "SELECT a.attname FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s "
        "AND a.attnum > 0 AND not attisdropped"
    )

    args = table.schema.name, table.name

    cursor.execute(query, args)

    return map(first, cursor.fetchall())


def refine_datapackage(cursor, raw_datapackage):
    dns, value_rows = zip(*raw_datapackage.rows)

    entity_ids = dns_to_entity_ids(cursor, list(dns))

    refined_value_rows = map(refine_values, value_rows)

    refined_rows = zip(entity_ids, refined_value_rows)

    return DataPackage(
        raw_datapackage.granularity, raw_datapackage.timestamp,
        raw_datapackage.trend_names, refined_rows
    )


def get_data_types(cursor, table, column_names):
    return [
        get_data_type(cursor, table, column_name)
        for column_name in column_names
    ]


def get_data_type(cursor, table, column_name):
    query = (
        "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type "
        "FROM pg_class c, pg_attribute a, pg_namespace n "
        "WHERE c.relname = %s "
        "AND n.nspname = %s "
        "AND a.attname = %s "
        "AND a.attrelid = c.oid "
        "AND c.relnamespace = n.oid"
    )

    args = table.name, table.schema.name, column_name

    cursor.execute(query, args)

    if cursor.rowcount > 0:
        return cursor.fetchone()[0]
    else:
        raise Exception(
            "No such column: {0}.{1}".format(table.name, column_name)
        )


def refine_values(raw_values):
    return [refine_value(value) for value in raw_values]


def refine_value(value):
    if len(value) == 0:
        return None
    elif type(value) is tuple:
        return ",".join(value)
    else:
        return value
