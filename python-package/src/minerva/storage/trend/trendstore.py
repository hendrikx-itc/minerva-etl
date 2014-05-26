# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from datetime import datetime
import logging
from functools import partial
from operator import attrgetter, methodcaller
import StringIO
from itertools import chain

import pytz
import psycopg2

from minerva.util import k, first, compose, no_op
from minerva.db.error import NoCopyInProgress, NoSuchTable, \
    NoSuchColumnError, UniqueViolation, DataTypeMismatch, DuplicateTable
from minerva.db.query import Table, Column, Eq, column_exists, ands
from minerva.directory.helpers_v4 import get_datasource_by_id, \
    get_entitytype_by_id, get_entity, dns_to_entity_ids
from minerva.storage.generic import datatype, format_value, escape_value
from minerva.db.error import translate_postgresql_exception, \
    translate_postgresql_exceptions
from minerva.db.dbtransaction import DbTransaction, DbAction, \
    insert_before, replace, drop_action
from minerva.storage.trend.tables import DATA_TABLE_POSTFIXES
from minerva.storage.trend import schema
from minerva.storage.trend.datapackage import DataPackage
from minerva.storage.trend.granularity import create_granularity, \
    ensure_granularity
from minerva.storage.trend.tables import create_column
from minerva.storage.trend.partition import Partition
from minerva.storage.trend.partitioning import Partitioning

LARGE_BATCH_THRESHOLD = 10


class TrendStore(object):
    """
    All data belonging to a specific datasource, entitytype and granularity.
    """
    def __init__(self, datasource, entitytype, granularity, partition_size,
                 type):
        self.id = None
        self.datasource = datasource
        self.entitytype = entitytype
        self.granularity = granularity
        self.partition_size = partition_size
        self.type = type
        self.version = 4
        self.partitioning = Partitioning(partition_size)

    def __str__(self):
        return self.make_table_basename()

    def make_table_basename(self):
        granularity_name = DATA_TABLE_POSTFIXES.get(
            self.granularity.name, self.granularity.name)

        return "{}_{}_{}".format(self.datasource.name, self.entitytype.name,
                granularity_name)

    def make_table_name(self, timestamp):
        table_basename = self.make_table_basename()

        if self.type == "view":
            return table_basename
        else:
            index = self.partitioning.index(timestamp)

            return "{}_{}".format(table_basename, index)

    def base_table(self):
        return Table("trend", self.make_table_basename())

    def partition(self, timestamp):
        if self.type == "view":
            index = None
            start = None
            end = None
        else:
            index = self.partitioning.index(timestamp)
            start, end = self.partitioning.index_to_interval(index)

        name = self.make_table_name(timestamp)

        return Partition(index, name, self, start, end, 4)

    def index_to_interval(self, partition_index):
        return self.partitioning.index_to_interval(partition_index)

    def check_columns_exist(self, column_names, data_types):
        def f(cursor):
            base_table = self.base_table()

            for column_name, data_type in zip(column_names, data_types):
                if not column_exists(cursor, base_table, column_name):
                    create_column(cursor, base_table, column_name, data_type)

                    assure_trendstore_trend_link(cursor, self, column_name)

        return f

    def check_column_types(self, column_names, data_types):
        """
        Check if database column types match trend datatype and correct it if
        necessary.
        """
        def f(cursor):
            table = self.base_table()
            current_data_types = get_data_types(cursor, table, column_names)

            changes = []

            for column_name, current_data_type, data_type in \
                    zip(column_names, current_data_types, data_types):
                required_data_type = datatype.max_datatype(current_data_type, data_type)

                if required_data_type != current_data_type:
                    changes.append((column_name, required_data_type))

                    logging.info(
                        "Column {0:s} requires change from type {1} to {2}".format(
                            column_name, current_data_type, required_data_type))

            query = (
                "SELECT trend.modify_trendstore_columns("
                "%s, "
                "%s::trend.column_info[]"
                ")")
            args = self.id, changes

            cursor.execute(query, args)

        return f

    def table_names(self, start, end):
        timestamps = self.granularity.range(start, end)

        table_names = map(self.make_table_name, timestamps)

        #HACK for dealing with intervals that are small but span two tables
        # (e.g. 2012-1-5 0:00 - 2012-1-5 1:00 for qtr tables)
        end_table = self.make_table_name(end)

        table_names.append(end_table)

        return list(set(table_names))

    def tables(self, start, end):
        make_table = partial(Table, "trend")
        table_names = self.table_names(start, end)

        return map(make_table, table_names)

    def get_trend(self, cursor, trend_name):
        query = (
            "SELECT t.id, t.name "
            "FROM trend.trendstore_trend_link ttl "
            "JOIN trend.trend t ON t.id = ttl.trend_id "
            "WHERE ttl.trendstore_id = %s AND t.name = %s")

        args = self.id, trend_name

        cursor.execute(query, args)

        if cursor.rowcount > 0:
            return cursor.fetchone()

    def get_trends(self, cursor):
        query = (
            "SELECT t.id, t.name FROM trend.trendstore_trend_link ttl "
            "JOIN trend.trend t ON t.id = ttl.trend_id "
            "WHERE ttl.trendstore_id = %s")

        args = (self.id, )

        cursor.execute(query, args)

        return cursor.fetchall()

    def create(self, cursor):
        column_names = ["datasource_id", "entitytype_id", "granularity",
                "partition_size", "type", "version"]

        columns = map(Column, column_names)

        args = (self.datasource.id, self.entitytype.id, self.granularity.name,
                self.partition_size, self.type, self.version)

        query = schema.trendstore.insert(columns).returning("id")

        query.execute(cursor, args)

        trendstore_id, = cursor.fetchone()

        self.id = trendstore_id

        return self

    def save(self, cursor):
        if self.id is None:
            return self.create(cursor)
        else:
            args = (self.datasource.id, self.entitytype.id, self.granularity.name,
                    self.partition_size, self.type, self.version, self.id)

            query = (
                "UPDATE trend.trendstore SET "
                    "datasource_id = %s, "
                    "entitytype_id = %s, "
                    "granularity = %s, "
                    "partition_size = %s, "
                    "type = %s, "
                    "version = %s "
                "WHERE id = %s")

            cursor.execute(query, args)

            return self

    column_names = ["id", "datasource_id", "entitytype_id", "granularity",
            "partition_size", "type", "version"]

    columns = map(Column, column_names)

    get_query = schema.trendstore.select(columns).where_(ands([
        Eq(Column("datasource_id")),
        Eq(Column("entitytype_id")),
        Eq(Column("granularity"))]))

    get_by_id_query = schema.trendstore.select(columns).where_(Eq(Column("id")))

    @classmethod
    def get(cls, cursor, datasource, entitytype, granularity):
        args = datasource.id, entitytype.id, granularity.name

        cls.get_query.execute(cursor, args)

        if cursor.rowcount > 1:
            raise Exception("more than 1 ({}) trendstore matches".format(
                    cursor.rowcount))
        elif cursor.rowcount == 1:
            trendstore_id, datasource_id, entitytype_id, granularity_str, \
                    partition_size, type, version = cursor.fetchone()

            trendstore = TrendStore(datasource, entitytype, granularity,
                    partition_size, type)

            trendstore.id = trendstore_id

            return trendstore

    @classmethod
    def get_by_id(cls, cursor, id):
        args = (id,)

        cls.get_by_id_query.execute(cursor, args)

        if cursor.rowcount == 1:
            trendstore_id, datasource_id, entitytype_id, granularity_str, \
                    partition_size, type, version = cursor.fetchone()

            datasource = get_datasource_by_id(cursor, datasource_id)
            entitytype = get_entitytype_by_id(cursor, entitytype_id)

            granularity = create_granularity(granularity_str)

            trendstore = TrendStore(datasource, entitytype, granularity,
                    partition_size, type)

            trendstore.id = trendstore_id

            return trendstore

    def has_trend(self, cursor, trend_name):
        query = (
            "SELECT 1 FROM trend.trendstore_trend_link ttl "
            "JOIN trend.trend t ON t.id = ttl.trend_id "
            "WHERE ttl.trendstore_id = %s AND t.name = %s")

        args = self.id, trend_name

        cursor.execute(query, args)

        return cursor.rowcount > 0

    def store(self, datapackage):
        if datapackage.is_empty():
            return DbTransaction()
        else:
            partition = self.partition(datapackage.timestamp)
            return store(partition, datapackage)

    def store_raw(self, raw_datapackage):
        if raw_datapackage.is_empty():
            return DbTransaction()

        if len(raw_datapackage.rows) <= LARGE_BATCH_THRESHOLD:
            insert_action = BatchInsert
        else:
            insert_action = CopyFrom

        return DbTransaction(
            RefineRawDataPackage(k(raw_datapackage)),
            SetTimestamp(read("datapackage")),
            SetPartition(self),
            GetTimestamp(),
            insert_action(read("partition"), read("datapackage")),
            MarkModified(read("partition"), read("timestamp"))
        )

    def clear_timestamp(self, timestamp):
        def f(cursor):
            query = (
                "DELETE FROM {} "
                "WHERE timestamp = %s").format(self.base_table().render())
            args = timestamp,

            cursor.execute(query, args)

        return f


def assure_trendstore_trend_link(cursor, trendstore, trend_name):
    if not trendstore.has_trend(cursor, trend_name):
        trend = trendstore.get_trend(cursor, trend_name)

        if not trend:
            trend = create_trend(cursor, trend_name)
            logging.info("created trend {}".format(trend_name))

        trend_id = trend[0]

        link_trend_to_trendstore(cursor, trendstore, trend_id)
        logging.info("linked trend {} to trendstore {}".format(
                trend_name, trendstore))


def link_trend_to_trendstore(cursor, trendstore, trend_id):
    query = (
        "INSERT INTO trend.trendstore_trend_link (trendstore_id, trend_id) "
        "VALUES (%s, %s)")

    args = trendstore.id, trend_id

    cursor.execute(query, args)


def create_trend(cursor, name, description=""):
    query = (
        "INSERT INTO trend.trend (name, description) "
        "VALUES (%s, %s) "
        "RETURNING id")

    args = name, description

    cursor.execute(query, args)

    trend_id, = cursor.fetchone()

    return trend_id, name


def store_raw(datasource, raw_datapackage):
    if raw_datapackage.is_empty():
        return DbTransaction()

    if len(raw_datapackage.rows) <= LARGE_BATCH_THRESHOLD:
        insert_action = BatchInsert
    else:
        insert_action = CopyFrom

    dn = raw_datapackage.rows[0][0]

    return DbTransaction(
        RefineRawDataPackage(k(raw_datapackage)),
        SetTimestamp(read("datapackage")),
        ExtractPartition(datasource, dn),
        GetTimestamp(),
        insert_action(read("partition"), read("datapackage")),
        MarkModified(read("partition"), read("timestamp")))


read = partial(methodcaller, 'get')


def get_args(*getters):
    def f(state):
        return [getter(state) for getter in getters]

    return f


class RefineRawDataPackage(DbAction):
    def __init__(self, raw_datapackage):
        self.raw_datapackage = raw_datapackage

    def execute(self, cursor, state):
        raw_datapackage = self.raw_datapackage(state)

        try:
            state["datapackage"] = refine_datapackage(cursor, raw_datapackage)
        except UniqueViolation:
            return no_op


class SetPartition(DbAction):
    def __init__(self, trendstore):
        self.trendstore = trendstore

    def execute(self, cursor, state):
        timestamp = state["timestamp"]

        state["partition"] = self.trendstore.partition(timestamp)


class ExtractPartition(DbAction):
    def __init__(self, datasource, dn):
        self.datasource = datasource
        self.dn = dn

    def execute(self, cursor, state):
        entity = get_entity(cursor, self.dn)
        entitytype = get_entitytype_by_id(cursor, entity.entitytype_id)
        datapackage = state["datapackage"]

        trendstore = TrendStore.get(cursor, self.datasource, entitytype,
                datapackage.granularity)

        if not trendstore:
            partition_size = 86400

            trendstore = TrendStore(self.datasource, entitytype,
                    datapackage.granularity, partition_size, "table").create(cursor)

        partition = trendstore.partition(datapackage.timestamp)

        logging.debug(partition.name)

        state["partition"] = partition


class SetTimestamp(DbAction):
    def __init__(self, datapackage):
        self.datapackage = datapackage

    def execute(self, cursor, state):
        datapackage = self.datapackage(state)

        state["timestamp"] = datapackage.timestamp


def store(partition, datapackage):
    if len(datapackage.rows) <= LARGE_BATCH_THRESHOLD:
        insert_action = BatchInsert
    else:
        insert_action = CopyFrom

    transaction = DbTransaction(
        GetTimestamp(),
        insert_action(k(partition), k(datapackage)),
        MarkModified(k(partition), k(datapackage.timestamp)))

    return transaction


class GetTimestamp(DbAction):
    def execute(self, cursor, state):
        state["modified"] = get_timestamp(cursor)


class MarkModified(DbAction):
    def __init__(self, partition, timestamp):
        self.partition = partition
        self.timestamp = timestamp

    def execute(self, cursor, state):
        partition = self.partition(state)
        timestamp = self.timestamp(state)

        try:
            mark_modified(cursor, partition.table(), timestamp,
                    state["modified"])
        except UniqueViolation:
            return no_op


class CopyFrom(DbAction):
    def __init__(self, partition, datapackage):
        self.partition = partition
        self.datapackage = datapackage

    def execute(self, cursor, state):
        partition = self.partition(state)
        datapackage = self.datapackage(state)

        try:
            store_copy_from(cursor, partition.table(), datapackage,
                    state["modified"])
        except NoCopyInProgress:
            return no_op
        except NoSuchTable:
            data_types = compose(DataPackage.deduce_data_types, self.datapackage)
            trend_names = compose(attrgetter("trend_names"), self.datapackage)
            fix = CreatePartition(self.partition, trend_names, data_types)
            return insert_before(fix)
        except NoSuchColumnError:
            data_types = compose(DataPackage.deduce_data_types, self.datapackage)
            trend_names = compose(attrgetter("trend_names"), self.datapackage)
            fix = CheckColumnsExist(self.partition, trend_names, data_types)
            return insert_before(fix)
        except UniqueViolation:
            fix = Update(self.partition, self.datapackage)
            return replace(fix)
        except DataTypeMismatch:
            data_types = compose(DataPackage.deduce_data_types, self.datapackage)
            trend_names = compose(attrgetter("trend_names"), self.datapackage)
            fix = CheckColumnTypes(self.partition, trend_names, data_types)
            return insert_before(fix)


class BatchInsert(DbAction):
    def __init__(self, partition, datapackage):
        self.partition = partition
        self.datapackage = datapackage

    def execute(self, cursor, state):
        partition = self.partition(state)
        datapackage = self.datapackage(state)

        try:
            try:
                store_batch_insert(cursor, partition.table(), datapackage,
                        state["modified"])
            except Exception as exc:
                logging.debug("exception: {}".format(type(exc).__name__))
                raise exc
        except NoSuchTable:
            data_types = compose(DataPackage.deduce_data_types, self.datapackage)
            trend_names = compose(attrgetter("trend_names"), self.datapackage)
            fix = CreatePartition(self.partition, trend_names, data_types)
            return insert_before(fix)
        except NoSuchColumnError:
            data_types = compose(DataPackage.deduce_data_types, self.datapackage)
            trend_names = compose(attrgetter("trend_names"), self.datapackage)
            fix = CheckColumnsExist(self.partition, trend_names, data_types)
            return insert_before(fix)
        except UniqueViolation:
            fix = Update(self.partition, self.datapackage)
            return replace(fix)
        except DataTypeMismatch:
            data_types = compose(DataPackage.deduce_data_types, self.datapackage)
            trend_names = compose(attrgetter("trend_names"), self.datapackage)
            fix = CheckColumnTypes(self.partition, trend_names, data_types)
            return insert_before(fix)


class Update(DbAction):
    def __init__(self, partition, datapackage):
        self.partition = partition
        self.datapackage = datapackage

    def execute(self, cursor, state):
        partition = self.partition(state)
        datapackage = self.datapackage(state)

        try:
            store_update(cursor, partition.table(), datapackage, state["modified"])
        except NoSuchTable:
            data_types = compose(DataPackage.deduce_data_types, self.datapackage)
            trend_names = compose(attrgetter("trend_names"), self.datapackage)
            fix = CreatePartition(self.partition, trend_names, data_types)
            return insert_before(fix)
        except NoSuchColumnError:
            data_types = compose(DataPackage.deduce_data_types, self.datapackage)
            trend_names = compose(attrgetter("trend_names"), self.datapackage)
            fix = CheckColumnsExist(self.partition, trend_names, data_types)
            return insert_before(fix)
        except DataTypeMismatch:
            data_types = compose(DataPackage.deduce_data_types, self.datapackage)
            trend_names = compose(attrgetter("trend_names"), self.datapackage)
            fix = CheckColumnTypes(self.partition, trend_names, data_types)
            return insert_before(fix)


class CheckColumnsExist(DbAction):
    def __init__(self, partition, column_names, data_types):
        self.partition = partition
        self.column_names = column_names
        self.data_types = data_types

    def execute(self, cursor, state):
        partition = self.partition(state)
        column_names = self.column_names(state)
        data_types = self.data_types(state)

        partition.check_columns_exist(column_names, data_types)(cursor)


class CreatePartition(DbAction):
    def __init__(self, partition, trend_names, data_types):
        self.partition = partition
        self.trend_names = trend_names
        self.data_types = data_types

    def execute(self, cursor, state):
        partition = self.partition(state)
        trend_names = self.trend_names(state)
        data_types = self.data_types(state)

        try:
            partition.create(cursor)
        except DuplicateTable:
            return drop_action()

        partition.check_columns_exist(trend_names, data_types)(cursor)


class CheckColumnTypes(DbAction):
    def __init__(self, partition, trend_names, data_types):
        self.partition = partition
        self.trend_names = trend_names
        self.data_types = data_types

    def execute(self, cursor, state):
        partition = self.partition(state)
        trend_names = self.trend_names(state)
        data_types = self.data_types(state)

        partition.check_column_types(trend_names, data_types)(cursor)


class DeleteBySubQuery(DbAction):
    def __init__(self, table, timestamp, entityselection, trend_names):
        self.table = table
        self.timestamp = timestamp
        self.entityselection = entityselection
        self.trend_names = trend_names

    def execute(self, cursor, state):
        table = self.table(state)
        timestamp = self.timestamp(state)
        entityselection = self.entityselection(state)
        trend_names = self.trend_names(state)

        actual_column_names = set(get_column_names(cursor, table))

        required_column_names = schema.system_columns_set | set(trend_names)

        if required_column_names != actual_column_names:
            return drop_action()

        try:
            delete_by_entityselection(cursor, table, timestamp, entityselection)
        except NoSuchTable:
            return drop_action()


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
    copy_from_file = create_copy_from_file(datapackage.timestamp, modified,
            datapackage.rows)

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
    return (create_copy_from_line(timestamp, modified, data_row)
            for data_row in data_rows)


def create_copy_from_line(timestamp, modified, data_row):
    entity_id, values = data_row

    trend_value_part = "\t".join(escape_value(format_value(value)) for value in values)

    return u"{0:d}\t'{1!s}'\t'{2!s}'\t{3}\n".format(entity_id,
            timestamp.isoformat(), modified.isoformat(), trend_value_part)


def create_copy_from_query(table, trend_names):
    column_names = chain(schema.system_columns, trend_names)

    quote = partial(str.format, '"{}"')

    query = "COPY {0}({1}) FROM STDIN".format(table.render(),
        ",".join(map(quote, column_names)))

    return query


@translate_postgresql_exceptions
def mark_modified(cursor, table, timestamp, modified):
    args = table.name, timestamp, modified

    cursor.callproc("trend.mark_modified", args)


def store_update(cursor, table, datapackage, modified):
    tmp_table = create_temp_table_from(cursor, table)

    store_copy_from(cursor, tmp_table, datapackage, modified)

    # Update existing records
    store_using_update(cursor, tmp_table, table, datapackage.trend_names,
            modified)

    # Fill in missing records
    store_using_tmp(cursor, tmp_table, table, datapackage.trend_names, modified)


def store_using_update(cursor, tmp_table, table, column_names, modified):
    set_columns = ", ".join("\"{0}\"={1}.\"{0}\"".format(name, tmp_table.render())
            for name in column_names)

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


def store_using_tmp(cursor, tmp_table, table, column_names, modified):
    """
    Store the data using the PostgreSQL specific COPY FROM command and a
    temporary table. The temporary table is joined against the target table
    to make sure only new records are inserted.
    """
    all_column_names = ['entity_id', 'timestamp']
    all_column_names.extend(column_names)

    tmp_column_names = ", ".join('tmp."{0}"'.format(name)
            for name in all_column_names)
    dest_column_names = ", ".join('"{0}"'.format(name)
            for name in all_column_names)

    insert_query = (
        "INSERT INTO {table} ({dest_columns}) "
        "SELECT {tmp_columns} FROM {tmp_table} AS tmp "
        "LEFT JOIN {table} ON "
            "tmp.\"timestamp\" = {table}.\"timestamp\" "
            "AND tmp.entity_id = {table}.entity_id "
        "WHERE {table}.entity_id IS NULL").format(
            table=table.render(),
            dest_columns=dest_column_names,
            tmp_columns=tmp_column_names,
            tmp_table=tmp_table.render())

    try:
        cursor.execute(insert_query)
    except psycopg2.Error as exc:
        raise translate_postgresql_exception(exc)


def store_batch_insert(cursor, table, datapackage, modified):
    column_names = ["entity_id", "timestamp", "modified"]
    column_names.extend(datapackage.trend_names)

    dest_column_names = ",".join('"{0}"'.format(column_name)
            for column_name in column_names)

    parameters = ", ".join(["%s"] * len(column_names))

    query = (
        "INSERT INTO {0} ({1}) "
        "VALUES ({2})").format(table.render(), dest_column_names, parameters)

    rows = [(entity_id, datapackage.timestamp, modified) + tuple(values)
            for entity_id, values in datapackage.rows]

    logging.debug(cursor.mogrify(query, first(rows)))

    try:
        cursor.executemany(query, rows)
    except psycopg2.DatabaseError as exc:
        m = str(exc)
        if m.find("violates check constraint") > -1:
            print(cursor.mogrify(query, first(rows)))
            print(m)

        raise translate_postgresql_exception(exc)


def create_temp_table_from(cursor, table):
    """
    Create a temporary table that is like `table` and return the temporary
    table name.
    """
    tmp_table = Table("tmp_{0}".format(table.name))

    query = (
        "CREATE TEMPORARY TABLE {0} (LIKE {1}) "
        "ON COMMIT DROP").format(tmp_table.render(), table.render())

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
        "AND a.attnum > 0 AND not attisdropped")

    args = table.schema.name, table.name

    cursor.execute(query, args)

    return map(first, cursor.fetchall())


def delete_by_entityselection(cursor, table, timestamp, entityselection):
    """
    Delete rows from table for a specific timestamp and entity_ids in
    entityselection
    """
    entityselection.create_temp_table(cursor, "entity_filter")

    delete_query = (
        "DELETE FROM {} d "
        "USING entity_filter f "
        "WHERE d.timestamp = %s AND f.entity_id = d.entity_id"
    ).format(table.render())

    args = (timestamp,)

    logging.debug(cursor.mogrify(delete_query, args))

    try:
        cursor.execute(delete_query, args)
    except psycopg2.DatabaseError as exc:
        raise translate_postgresql_exception(exc)


def refine_datapackage(cursor, raw_datapackage):
    dns, value_rows = zip(*raw_datapackage.rows)

    entity_ids = dns_to_entity_ids(cursor, list(dns))

    refined_value_rows = map(refine_values, value_rows)

    refined_rows = zip(entity_ids, refined_value_rows)

    timestamp = pytz.UTC.localize(datetime.strptime(raw_datapackage.timestamp,
            "%Y-%m-%dT%H:%M:%S"))

    granularity = ensure_granularity(raw_datapackage.granularity)

    return DataPackage(granularity, timestamp, raw_datapackage.trend_names,
            refined_rows)


def get_data_types(cursor, table, column_names):
    return [get_data_type(cursor, table, column_name)
            for column_name in column_names]


def get_data_type(cursor, table, column_name):
    query = (
        "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type "
        "FROM pg_class c, pg_attribute a, pg_namespace n "
        "WHERE c.relname = %s "
        "AND n.nspname = %s "
        "AND a.attname = %s "
        "AND a.attrelid = c.oid "
        "AND c.relnamespace = n.oid")

    args = table.name, table.schema.name, column_name

    cursor.execute(query, args)

    if cursor.rowcount > 0:
        return cursor.fetchone()[0]
    else:
        raise Exception("No such column: {0}.{1}".format(table.name, column_name))


def refine_values(raw_values):
    return [refine_value(value) for value in raw_values]


def refine_value(value):
    if len(value) == 0:
        return None
    elif type(value) is tuple:
        return ",".join(value)
    else:
        return value
