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
from io import StringIO
from itertools import chain

import psycopg2

from minerva.util import first, no_op, zip_apply, compose
from minerva.db.error import NoCopyInProgress, NoSuchTable, \
    NoSuchColumnError, UniqueViolation, DataTypeMismatch, DuplicateTable, \
    translate_postgresql_exception, translate_postgresql_exceptions
from minerva.db.query import Table, Column, Eq, ands
from minerva.db.util import create_temp_table_from, quote_ident
from minerva.directory import DataSource, EntityType
from minerva.db.dbtransaction import DbTransaction, DbAction, \
    insert_before, replace, drop_action
from minerva.storage.trend.tables import DATA_TABLE_POSTFIXES
from minerva.storage.trend import schema
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.partition import Partition
from minerva.storage.trend.partitioning import Partitioning
from minerva.storage.trend.trend import Trend, TrendDescriptor
from minerva.storage import datatype
from minerva.storage.valuedescriptor import ValueDescriptor

LARGE_BATCH_THRESHOLD = 10


class NoSuchTrendError(Exception):
    pass


class TrendStoreDescriptor():
    def __init__(
            self, data_source, entity_type, granularity, trend_descriptors,
            partition_size):
        self.data_source = data_source
        self.entity_type = entity_type
        self.granularity = granularity
        self.trend_descriptors = trend_descriptors
        self.partition_size = partition_size


class TrendStore():
    """
    All data belonging to a specific datasource, entitytype and granularity.
    """
    def __init__(
            self, id, data_source, entity_type, granularity, partition_size,
            type, trends):
        self.id = id
        self.data_source = data_source
        self.entity_type = entity_type
        self.granularity = granularity
        self.partition_size = partition_size
        self.type = type
        self.partitioning = Partitioning(partition_size)
        self.trends = trends

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
            "trendstore, %s::trend_directory.trend_descr[]"
            ") "
            "FROM trend_directory.trendstore "
            "WHERE id = %s"
        )

        args = trend_descriptors, self.id

        def f(cursor):
            cursor.execute(query, args)

        return f

    def check_data_types(self, trend_descriptors):
        """
        Check if database column types match trend data type and correct it if
        necessary.
        """
        query = (
            "SELECT trend_directory.assure_data_types("
            "trendstore, %s::trend_directory.trend_descr[]"
            ") "
            "FROM trend_directory.trendstore "
            "WHERE id = %s"
        )

        args = trend_descriptors, self.id

        def f(cursor):
            cursor.execute(query, args)

        return f

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
    def get_trends(cursor, trend_store_id):
        query = (
            "SELECT id, name, data_type, trendstore_id, description "
            "FROM trend_directory.trend "
            "WHERE trendstore_id = %s"
        )

        args = (trend_store_id, )

        cursor.execute(query, args)

        return [
            Trend(id, name, datatype.type_map[data_type], trendstore_id, description)
            for id, name, data_type, trendstore_id, description in cursor.fetchall()
        ]

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
                "SELECT * FROM trend_directory.create_trendstore("
                "%s, %s, %s, %s::trend_directory.trend_descr[], %s"
                ")"
            )

            cursor.execute(query, args)

            (
                trendstore_id, entitytype_id, datasource_id, granularity_str,
                partition_size, type, retention_period
            ) = cursor.fetchone()

            entity_type = EntityType.get(entitytype_id)(cursor)
            data_source = DataSource.get(datasource_id)(cursor)

            trends = TrendStore.get_trends(cursor, trendstore_id)

            return TrendStore(
                trendstore_id, data_source, entity_type,
                create_granularity(granularity_str), partition_size, type,
                trends
            )

        return f

    def save(self, cursor):
        if self.id is None:
            return self.create(cursor)
        else:
            args = (
                self.data_source.id, self.entity_type.id, self.granularity.name,
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

    @classmethod
    def get(cls, data_source, entity_type, granularity):
        def f(cursor):
            args = data_source.id, entity_type.id, str(granularity)

            get_query.execute(cursor, args)

            if cursor.rowcount > 1:
                raise Exception(
                    "more than 1 ({}) trendstore matches".format(
                        cursor.rowcount
                    )
                )
            elif cursor.rowcount == 1:
                (
                    trendstore_id, datasource_id, entitytype_id,
                    granularity_str, partition_size, type
                ) = cursor.fetchone()

                trends = TrendStore.get_trends(cursor, trendstore_id)

                return TrendStore(
                    trendstore_id, data_source, entity_type, granularity,
                    partition_size, type, trends
                )

        return f

    @classmethod
    def get_by_id(cls, id):
        def f(cursor):
            args = (id,)

            get_by_id_query.execute(cursor, args)

            if cursor.rowcount == 1:
                (
                    trendstore_id, datasource_id, entitytype_id,
                    granularity_str, partition_size, type
                ) = cursor.fetchone()

                data_source = DataSource.get(datasource_id)(cursor)
                entity_type = EntityType.get(entitytype_id)(cursor)

                trends = TrendStore.get_trends(cursor, id)

                granularity = create_granularity(granularity_str)

                return TrendStore(
                    trendstore_id, data_source, entity_type, granularity,
                    partition_size, type, trends
                )

        return f

    def has_trend(self, cursor, trend_name):
        query = (
            "SELECT 1 FROM trend_directory.trend "
            "WHERE trendstore_id = %s AND name = %s"
        )

        args = self.id, trend_name

        cursor.execute(query, args)

        return cursor.rowcount > 0

    def store(self, data_package):
        if data_package.is_empty():
            return DbTransaction()
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
                "SELECT trend_directory.cleare_timestamp(trendstore, %s) "
                "FROM trend_directory.trendstore "
                "WHERE id = %s"
            )

            args = timestamp, self.id

            cursor.execute(query, args)

        return f

    def get_string_parsers(self, trend_names):
        trend_by_name = {t.name: t for t in self.trends}

        def get_parser_by_trend_name(name):
            try:
                trend = trend_by_name[name]
            except KeyError:
                raise NoSuchTrendError('no trend with name {}'.format(name))
            else:
                data_type = trend.data_type

                return data_type.string_parser(data_type.string_parser_config())

        return [
            get_parser_by_trend_name(name)
            for name in trend_names
        ]

    def get_value_descriptors(self, trend_names):
        trend_by_name = {t.name: t for t in self.trends}

        def get_descriptor_by_trend_name(name):
            try:
                trend = trend_by_name[name]
            except KeyError:
                raise NoSuchTrendError('no trend with name {}'.format(name))
            else:
                data_type = trend.data_type

                return ValueDescriptor(
                    name, data_type, data_type.string_parser_config()
                )

        return [
            get_descriptor_by_trend_name(name)
            for name in trend_names
        ]

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

        :param conn: DBAPI2 database connection
        :param table: Name of table, including schema
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

            dest_column_names = ",".join(
                '"{0}"'.format(column_name)
                for column_name in column_names
            )

            parameters = ", ".join(["%s"] * len(column_names))

            query = (
                "INSERT INTO {0} ({1}) "
                "VALUES ({2})"
            ).format(table.render(), dest_column_names, parameters)

            value_parsers = self.get_string_parsers(data_package.trend_names)

            rows = [
                (entity_id, data_package.timestamp, modified) + tuple(values)
                for entity_id, values in data_package.refined_rows(value_parsers)(cursor)
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


class StoreState():
    def __init__(self, trend_store, data_package):
        self.trend_store = trend_store
        self.data_package = data_package
        self.modified = None


def trend_descriptors_from_data_package(data_package):
    return [
        TrendDescriptor(name, data_type, 'Created by CopyFrom')
        for name, data_type in zip(
            data_package.trend_names, data_package.deduce_data_types()
        )
    ]


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
        except NoSuchColumnError:
            return insert_before(CheckColumnsExist())
        except UniqueViolation:
            return replace(Update())
        except DataTypeMismatch:
            return insert_before(CheckDataTypes())


class BatchInsert(DbAction):
    def execute(self, cursor, state):
        try:
            state.trend_store.store_batch_insert(
                state.data_package,
                state.modified
            )(cursor)
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
            state.trend_store.store_update(
                state.data_package,
                state.modified
            )(cursor)
        except NoSuchTable:
            return insert_before(CreatePartition())
        except NoSuchColumnError:
            return insert_before(CheckColumnsExist())
        except DataTypeMismatch:
            return insert_before(CheckDataTypes())


class CheckColumnsExist(DbAction):
    def execute(self, cursor, state):
        state.trend_store.check_trends_exist(
            trend_descriptors_from_data_package(state.data_package)
        )(cursor)


class CreatePartition(DbAction):
    def execute(self, cursor, state):
        try:
            state.trend_store.partition(
                state.data_package.timestamp
            ).create(cursor)
        except DuplicateTable:
            return drop_action()


class CheckDataTypes(DbAction):
    def execute(self, cursor, state):
        state.trend_store.check_data_types(
            trend_descriptors_from_data_package(state.data_package)
        )(cursor)


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


def create_file(lines):
    copy_from_file = StringIO()

    copy_from_file.writelines(lines)

    copy_from_file.seek(0)

    return copy_from_file


create_copy_from_file = compose(create_file, create_copy_from_lines)
