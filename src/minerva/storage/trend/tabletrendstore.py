# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import logging
from itertools import chain

import psycopg2

from minerva.db.util import create_temp_table_from, quote_ident, create_file
from minerva.util import first, no_op, zip_apply, compose
from minerva.db.query import Table, Column, Eq, ands
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.partition import Partition
from minerva.storage.trend.partitioning import Partitioning
from minerva.storage.trend.tables import DATA_TABLE_POSTFIXES
from minerva.storage.trend import schema
from minerva.directory import DataSource, EntityType
from minerva.storage.trend.granularity import create_granularity
from minerva.db.error import NoCopyInProgress, NoSuchTable, \
    UniqueViolation, DuplicateTable, \
    translate_postgresql_exception, translate_postgresql_exceptions
from minerva.db.dbtransaction import DbTransaction, DbAction, \
    insert_before, replace, drop_action

LARGE_BATCH_THRESHOLD = 10


class TableTrendStoreDescriptor():
    def __init__(
            self, data_source, entity_type, granularity, trend_descriptors,
            partition_size):
        self.data_source = data_source
        self.entity_type = entity_type
        self.granularity = granularity
        self.trend_descriptors = trend_descriptors
        self.partition_size = partition_size


class TableTrendStore(TrendStore):
    column_names = [
        "id", "data_source_id", "entity_type_id", "granularity",
        "partition_size", "retention_period"
    ]

    columns = list(map(Column, column_names))

    get_query = schema.table_trend_store.select(columns).where_(ands([
        Eq(Column("data_source_id")),
        Eq(Column("entity_type_id")),
        Eq(Column("granularity"))
    ]))

    get_by_id_query = schema.table_trend_store.select(
        columns
    ).where_(Eq(Column("id")))

    def __init__(
            self, id_, data_source, entity_type, granularity, trends,
            partition_size):
        super().__init__(id_, data_source, entity_type, granularity, trends)
        self.partition_size = partition_size
        self.partitioning = Partitioning(partition_size)

    def __str__(self):
        return self.base_table_name()

    def base_table_name(self):
        granularity_str = str(self.granularity)

        postfix = DATA_TABLE_POSTFIXES.get(
            granularity_str, granularity_str
        )

        return "{}_{}_{}".format(
            self.data_source.name, self.entity_type.name, postfix
        )

    def partition_table_name(self, timestamp):
        return "{}_{}".format(
            self.base_table_name(),
            self.partitioning.index(timestamp)
        )

    def base_table(self):
        return Table("trend", self.base_table_name())

    def partition(self, timestamp):
        index = self.partitioning.index(timestamp)

        return Partition(index, self)

    def index_to_interval(self, partition_index):
        return self.partitioning.index_to_interval(partition_index)

    def check_trends_exist(self, trend_descriptors):
        query = (
            "SELECT trend_directory.assure_trends_exist("
            "table_trend_store, %s::trend_directory.trend_descr[]"
            ") "
            "FROM trend_directory.table_trend_store "
            "WHERE id = %s"
        )

        args = trend_descriptors, self.id

        def f(cursor):
            cursor.execute(query, args)

        return f

    def ensure_data_types(self, trend_descriptors):
        """
        Check if database column types match trend data type and correct it if
        necessary.
        """
        query = (
            "SELECT trend_directory.assure_data_types("
            "table_trend_store, %s::trend_directory.trend_descr[]"
            ") "
            "FROM trend_directory.table_trend_store "
            "WHERE id = %s"
        )

        args = trend_descriptors, self.id

        def f(cursor):
            cursor.execute(query, args)

        return f

    @staticmethod
    def create(descriptor):
        def f(cursor):
            args = (
                descriptor.data_source.name,
                descriptor.entity_type.name,
                str(descriptor.granularity),
                descriptor.trend_descriptors,
                descriptor.partition_size
            )

            query = (
                "SELECT * FROM trend_directory.create_table_trend_store("
                "%s, %s, %s, %s::trend_directory.trend_descr[], %s"
                ")"
            )

            cursor.execute(query, args)

            return TableTrendStore.from_record(cursor.fetchone())(cursor)

        return f

    @staticmethod
    def from_record(record):
        """
        Return function that can instantiate a TableTrendStore from a
        table_trend_store type record.
        :param record: An iterable that represents a table_trend_store record
        :return: function that creates and returns TableTrendStore object
        """
        def f(cursor):
            (
                trend_store_id, entity_type_id, data_source_id, granularity_str,
                partition_size, retention_period
            ) = record

            entity_type = EntityType.get(entity_type_id)(cursor)
            data_source = DataSource.get(data_source_id)(cursor)

            trends = TableTrendStore.get_trends(cursor, trend_store_id)

            return TableTrendStore(
                trend_store_id, data_source, entity_type,
                create_granularity(granularity_str), trends, partition_size
            )

        return f

    @classmethod
    def get(cls, data_source, entity_type, granularity):
        def f(cursor):
            args = data_source.id, entity_type.id, str(granularity)

            cls.get_query.execute(cursor, args)

            if cursor.rowcount > 1:
                raise Exception(
                    "more than 1 ({}) trend store matches".format(
                        cursor.rowcount
                    )
                )
            elif cursor.rowcount == 1:
                return TableTrendStore.from_record(cursor.fetchone())(cursor)

        return f

    @classmethod
    def get_by_id(cls, id_):
        def f(cursor):
            args = (id_,)

            cls.get_by_id_query.execute(cursor, args)

            if cursor.rowcount == 1:
                return TableTrendStore.from_record(cursor.fetchone())(cursor)

        return f

    def save(self, cursor):
        args = (
            self.data_source.id, self.entity_type.id, self.granularity.name,
            self.partition_size, self.id
        )

        query = (
            "UPDATE trend_directory.trend_store SET "
            "data_source_id = %s, "
            "entity_type_id = %s, "
            "granularity = %s, "
            "partition_size = %s "
            "WHERE id = %s"
        )

        cursor.execute(query, args)

        return self

    def store(self, data_package):
        if data_package.is_empty():
            return DbTransaction(None, [])
        else:
            if len(data_package.rows) <= LARGE_BATCH_THRESHOLD:
                insert_action = BatchInsert
            else:
                insert_action = CopyFrom

            return DbTransaction(
                StoreState(self, data_package),
                [
                    SetModified(),
                    insert_action(),
                    MarkModified()
                ]
            )

    def clear_timestamp(self, timestamp):
        def f(cursor):
            query = (
                "SELECT trend_directory.clear_timestamp(trend_store, %s) "
                "FROM trend_directory.trend_store "
                "WHERE id = %s"
            )

            args = timestamp, self.id

            cursor.execute(query, args)

        return f

    def _store_copy_from(self, table, data_package, modified):
        def f(cursor):
            value_parsers = self.get_string_parsers(data_package.trend_names)

            value_descriptors = self.get_value_descriptors(
                data_package.trend_names
            )

            copy_from_file = create_copy_from_file(
                data_package.timestamp, modified,
                data_package.refined_rows(value_parsers)(cursor),
                value_descriptors
            )

            copy_from_query = create_copy_from_query(
                table, data_package.trend_names
            )

            logging.debug(copy_from_query)

            try:
                cursor.copy_expert(copy_from_query, copy_from_file)
            except psycopg2.DatabaseError as exc:
                if exc.pgcode is None and str(exc).find("no COPY in progress") != -1:
                    # Might happen after database connection loss
                    raise NoCopyInProgress()
                else:
                    raise translate_postgresql_exception(exc)

        return f

    def store_copy_from(self, data_package, modified):
        """
        Store the data using the PostgreSQL specific COPY FROM command

        :param data_package: A DataPackage object
        """
        return self._store_copy_from(
            self.partition(data_package.timestamp).table(),
            data_package,
            modified
        )

    def store_batch_insert(self, data_package, modified):
        def f(cursor):
            table = self.partition(data_package.timestamp).table()

            column_names = ["entity_id", "timestamp", "modified"]
            column_names.extend(data_package.trend_names)

            columns_part = ",".join(
                map(quote_ident, column_names)
            )

            parameters = ", ".join(['%s'] * len(column_names))

            query = (
                "INSERT INTO {0} ({1}) "
                "VALUES ({2})"
            ).format(table.render(), columns_part, parameters)

            value_parsers = self.get_string_parsers(data_package.trend_names)

            rows = [
                (entity_id, data_package.timestamp, modified) + tuple(values)
                for entity_id, values
                in data_package.refined_rows(value_parsers)(cursor)
            ]

            try:
                cursor.executemany(query, rows)
            except psycopg2.DatabaseError as exc:
                logging.debug(cursor.query)

                raise translate_postgresql_exception(exc)

        return f

    def store_update(self, data_package, modified):
        def f(cursor):
            table = self.partition(data_package.timestamp).table()

            tmp_table = create_temp_table_from(cursor, table)

            self._store_copy_from(tmp_table, data_package, modified)(cursor)

            # Update existing records
            self._update_existing_from_tmp(
                tmp_table, table, data_package.trend_names, modified
            )(cursor)

            # Fill in missing records
            self._copy_missing_from_tmp(
                tmp_table, table, data_package.trend_names
            )(cursor)

        return f

    @staticmethod
    def _update_existing_from_tmp(tmp_table, table, column_names, modified):
        def f(cursor):
            set_columns = ", ".join(
                '"{0}"={1}."{0}"'.format(name, tmp_table.render())
                for name in column_names
            )

            update_query = (
                'UPDATE {0} SET modified=greatest(%s, {0}.modified), {1} '
                'FROM {2} '
                'WHERE {0}.entity_id={2}.entity_id '
                'AND {0}."timestamp"={2}."timestamp"'
            ).format(table.render(), set_columns, tmp_table.render())

            args = (modified, )

            try:
                cursor.execute(update_query, args)
            except psycopg2.DatabaseError as exc:
                raise translate_postgresql_exception(exc)

        return f

    @staticmethod
    def _copy_missing_from_tmp(tmp_table, table, column_names):
        """
        Store the data using the PostgreSQL specific COPY FROM command and a
        temporary table. The temporary table is joined against the target table
        to make sure only missing records (based on entity_id, timestamp
        combination) are inserted.
        """
        def f(cursor):
            all_column_names = ['entity_id', 'timestamp', 'modified']
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
                'INSERT INTO {table} ({dest_columns}) '
                'SELECT {tmp_columns} FROM {tmp_table} AS tmp '
                'LEFT JOIN {table} ON '
                'tmp."timestamp" = {table}."timestamp" '
                'AND tmp.entity_id = {table}.entity_id '
                'WHERE {table}.entity_id IS NULL'
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

        return f

    @translate_postgresql_exceptions
    def mark_modified(self, timestamp, modified):
        def f(cursor):
            args = self.id, timestamp, modified

            cursor.callproc("trend_directory.mark_modified", args)

        return f


class StoreState():
    def __init__(self, trend_store, data_package):
        self.trend_store = trend_store
        self.data_package = data_package
        self.modified = None


class SetModified(DbAction):
    def execute(self, cursor, state):
        state.modified = get_timestamp(cursor)


class MarkModified(DbAction):
    def execute(self, cursor, state):
        try:
            state.trend_store.mark_modified(
                state.data_package.timestamp,
                state.modified
            )(cursor)
        except UniqueViolation:
            return no_op


class CopyFrom(DbAction):
    def execute(self, cursor, state):
        try:
            state.trend_store.store_copy_from(
                state.data_package,
                state.modified
            )(cursor)
        except NoCopyInProgress:
            return no_op
        except NoSuchTable:
            return insert_before(CreatePartition())
        except UniqueViolation:
            return replace(Update())


class BatchInsert(DbAction):
    def execute(self, cursor, state):
        try:
            state.trend_store.store_batch_insert(
                state.data_package,
                state.modified
            )(cursor)
        except NoSuchTable:
            return insert_before(CreatePartition())
        except UniqueViolation:
            return replace(Update())


class Update(DbAction):
    def execute(self, cursor, state):
        try:
            state.trend_store.store_update(
                state.data_package,
                state.modified
            )(cursor)
        except NoSuchTable:
            return insert_before(CreatePartition())


class CreatePartition(DbAction):
    def execute(self, cursor, state):
        try:
            state.trend_store.partition(
                state.data_package.timestamp
            ).create(cursor)
        except DuplicateTable:
            return drop_action()


def get_timestamp(cursor):
    cursor.execute("SELECT NOW()")

    return first(cursor.fetchone())


def create_copy_from_query(table, trend_names):
    """Return SQL query that can be used in the COPY FROM command."""
    column_names = chain(schema.system_columns, trend_names)

    return "COPY {0}({1}) FROM STDIN".format(
        table.render(),
        ",".join(map(quote_ident, column_names))
    )


def create_copy_from_lines(timestamp, modified, rows, value_descriptors):
    value_mappers = [
        value_descriptor.serialize_to_string
        for value_descriptor in value_descriptors
    ]

    map_values = zip_apply(value_mappers)

    return (
        u"{0:d}\t'{1!s}'\t'{2!s}'\t{3}\n".format(
            entity_id,
            timestamp.isoformat(),
            modified.isoformat(),
            "\t".join(map_values(values))
        )
        for entity_id, values in rows
    )


create_copy_from_file = compose(create_file, create_copy_from_lines)
