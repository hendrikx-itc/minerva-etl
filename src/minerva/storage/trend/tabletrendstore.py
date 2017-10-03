# -*- coding: utf-8 -*-
import logging
from contextlib import closing
from itertools import chain
from typing import List, Callable
from datetime import datetime

import psycopg2

from minerva.db.util import create_temp_table_from, quote_ident, create_file
from minerva.storage import datatype
from minerva.util import first, zip_apply, compose
from minerva.db.query import Table, Column, Eq, ands, Any
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.trend import Trend, NoSuchTrendError
from minerva.storage.trend.partition import Partition
from minerva.storage.trend.partitioning import Partitioning

from minerva.storage.trend import schema
from minerva.directory import DataSource, EntityType
from minerva.storage.trend.granularity import create_granularity, Granularity
from minerva.db.error import NoCopyInProgress, \
    translate_postgresql_exception, translate_postgresql_exceptions

LARGE_BATCH_THRESHOLD = 10


class TableTrendStorePart:
    id_: int
    name: str
    trends: List[Trend]

    class Descriptor:
        def __init__(
                self, name: str, trend_descriptors: List[Trend.Descriptor]):
            self.name = name
            self.trend_descriptors = trend_descriptors

    def __init__(self, id_: int, table_trend_store, name: str, trends: List[Trend]):
        self.id = id_
        self.table_trend_store = table_trend_store
        self.name = name
        self.trends = trends

    def __str__(self):
        return self.base_table_name()

    @staticmethod
    def get_trends(cursor, trend_store_part_id):
        query = (
            "SELECT id, name, data_type, trend_store_part_id, description "
            "FROM trend_directory.trend "
            "WHERE trend_store_part_id = %s"
        )

        args = (trend_store_part_id, )

        cursor.execute(query, args)

        return [
            Trend(
                id_, name, datatype.registry[data_type], trend_store_id,
                description
            )
            for id_, name, data_type, trend_store_id, description
            in cursor.fetchall()
        ]

    @staticmethod
    def from_record(record, table_trend_store) -> Callable[[Any], Any]:
        """
        Return function that can instantiate a TableTrendStore from a
        table_trend_store type record.
        :param record: An iterable that represents a table_trend_store record
        :return: function that creates and returns TableTrendStore object
        """
        def f(cursor):
            (trend_store_part_id, trend_store_id, name) = record

            trends = TableTrendStorePart.get_trends(cursor, trend_store_part_id)

            return TableTrendStorePart(
                trend_store_part_id, table_trend_store, name, trends
            )

        return f

    def base_table_name(self):
        """
        Return the base/parent table name.

        :return: table name
        """
        return self.name

    def partition_table_name(self, timestamp: datetime):
        """
        Return the name of the partition corresponding with the provided
        timestamp.

        :param timestamp:
        :return: name of partition table
        """
        return "{}_{}".format(
            self.base_table_name(),
            self.table_trend_store.index(timestamp)
        )

    def partition(self, timestamp: datetime):
        index = self.table_trend_store.partitioning.index(timestamp)

        return Partition(index, self)

    def base_table(self):
        return Table("trend", self.base_table_name())

    def get_copy_serializers(self, trend_names):
        trend_by_name = {t.name: t for t in self.trends}

        def get_serializer_by_trend_name(name):
            try:
                trend = trend_by_name[name]
            except KeyError:
                raise NoSuchTrendError('no trend with name {}'.format(name))
            else:
                data_type = trend.data_type

                return data_type.string_serializer(
                    datatype.copy_from_serializer_config(data_type)
                )

        return [
            get_serializer_by_trend_name(name)
            for name in trend_names
        ]

    @classmethod
    def get_by_id(cls, id_):
        def f(cursor):
            args = (id_,)

            cls.get_by_id_query.execute(cursor, args)

            if cursor.rowcount == 1:
                return TableTrendStorePart.from_record(cursor.fetchone())(cursor)

        return f

    def check_trends_exist(self, trend_descriptors: List[Trend.Descriptor]) -> Callable[[Any], Any]:
        """
        Returns function that creates missing trends as described by
        'trend_descriptors' and returns a new TableTrendStore.

        :param trend_descriptors: A list with trend descriptors indicating the
        required trends and their data types.
        """
        """
        :param trend_descriptors:
        :return:
        """
        query = (
            "SELECT trend_directory.assure_table_trends_exist("
            "table_trend_store, %s::trend_directory.trend_descr[]"
            ") "
            "FROM trend_directory.table_trend_store "
            "WHERE id = %s"
        )

        args = trend_descriptors, self.id

        def f(cursor):
            cursor.execute(query, args)

            return TableTrendStorePart.get_by_id(self.id)(cursor)

        return f

    def _store_copy_from(self, table, data_package, modified):
        def f(cursor):
            serializers = self.get_copy_serializers(data_package.trend_names)

            copy_from_file = create_copy_from_file(
                data_package.timestamp, modified,
                data_package.refined_rows(cursor),
                serializers
            )

            copy_from_query = create_copy_from_query(
                table, data_package.trend_names
            )

            logging.debug(copy_from_query)

            try:
                cursor.copy_expert(copy_from_query, copy_from_file)
            except psycopg2.DatabaseError as exc:
                if exc.pgcode is None and str(exc).find(
                        "no COPY in progress") != -1:
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

            rows = [
                (entity_id, data_package.timestamp, modified) + tuple(values)
                for entity_id, values
                in data_package.refined_rows(cursor)
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

    def ensure_data_types(self, trend_descriptors: List[Trend.Descriptor]):
        """
        Check if database column types match trend data type and correct it if
        necessary.

        :param trend_descriptors: A list with trend descriptors indicating the
        required data type of the corresponding trends.
        """
        query = (
            "SELECT trend_directory.assure_data_types("
            "table_trend_store_part, %s::trend_directory.trend_descr[]"
            ") "
            "FROM trend_directory.table_trend_store_part "
            "WHERE id = %s"
        )

        args = trend_descriptors, self.id

        def f(cursor):
            cursor.execute(query, args)

        return f


class TableTrendStore(TrendStore):
    class Descriptor:
        data_source: DataSource
        entity_type: EntityType
        granularity: Granularity
        parts: List[TableTrendStorePart.Descriptor]
        partition_size: int

        def __init__(
                self, data_source: DataSource, entity_type: EntityType,
                granularity: Granularity,
                parts: List[TableTrendStorePart.Descriptor],
                partition_size: int):
            self.data_source = data_source
            self.entity_type = entity_type
            self.granularity = granularity
            self.parts = parts
            self.partition_size = partition_size

    partition_size: int
    partitioning: Partitioning
    parts: List[TableTrendStorePart]

    column_names = [
        "id", "entity_type_id", "data_source_id", "granularity",
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
            self, id_: int, data_source: DataSource, entity_type: EntityType,
            granularity: Granularity, partition_size: int, retention_period):
        TrendStore.__init__(
            self, id_, data_source, entity_type, granularity
        )
        self.partition_size = partition_size
        self.partitioning = Partitioning(partition_size)
        self.retention_period = retention_period
        self.parts = []

    def partition(self, part_name, timestamp: datetime):
        index = self._part_mapping[part_name].index(timestamp)

        return Partition(index, self)

    def index_to_interval(self, partition_index: int):
        return self.partitioning.index_to_interval(partition_index)

    @staticmethod
    def create(descriptor: Descriptor):
        def f(cursor):
            parts_sql = "ARRAY[{}]::trend_directory.table_trend_store_part_descr[]".format(
                ','.join([
                    "('{}', {})".format(
                        part.name,
                        'ARRAY[{}]::trend_directory.trend_descr[]'.format(
                            ','.join([
                                "('{}', '{}', '')".format(
                                    trend_descriptor.name,
                                    trend_descriptor.data_type.name,
                                    ''
                                )
                                for trend_descriptor in part.trend_descriptors
                            ]))
                    )
                    for part in descriptor.parts
                ]))

            args = (
                descriptor.data_source.name,
                descriptor.entity_type.name,
                str(descriptor.granularity),
                descriptor.partition_size
            )

            query = (
                "SELECT * FROM trend_directory.create_table_trend_store("
                "%s, %s, %s, %s, {parts}"
                ")"
            ).format(parts=parts_sql)

            cursor.execute(query, args)

            return TableTrendStore.from_record(cursor.fetchone())(cursor)

        return f

    @staticmethod
    def create_part(record, table_trend_store):
        trend_store_part_id, name, trend_store_id = record

        trends = TableTrendStorePart.get_trends(cursor, trend_store_part_id)

        TableTrendStorePart(trend_store_part_id, table_trend_store, name, trends)

    def load_parts(self, cursor):
        query = (
            "SELECT id, trend_store_id, name "
            "FROM trend_directory.table_trend_store_part "
            "WHERE trend_store_id = %s"
        )

        args = (self.id,)

        cursor.execute(query, args)

        self.parts = [
            TableTrendStorePart.from_record(record, self)(cursor)
            for record in cursor.fetchall()
        ]

        self._part_mapping = {
            part.name: part for part in self.parts
        }

        self._trend_part_mapping = {
            trend.name: part for part in self.parts for trend in part.trends
        }

        return self

    @staticmethod
    def from_record(record) -> Callable[[Any], Any]:
        """
        Return function that can instantiate a TableTrendStore from a
        table_trend_store type record.
        :param record: An iterable that represents a table_trend_store record
        :return: function that creates and returns TableTrendStore object
        """
        def f(cursor):
            (
                trend_store_id, entity_type_id, data_source_id,
                granularity_str, partition_size, retention_period
            ) = record

            entity_type = EntityType.get(entity_type_id)(cursor)
            data_source = DataSource.get(data_source_id)(cursor)

            return TableTrendStore(
                trend_store_id, data_source, entity_type,
                create_granularity(granularity_str), partition_size,
                retention_period
            ).load_parts(cursor)

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
        def f(conn):
            with closing(conn.cursor()) as cursor:
                modified = get_timestamp(cursor)

                part = self.parts[0]

                if len(data_package.rows) <= LARGE_BATCH_THRESHOLD:
                    part.store_batch_insert(
                        data_package,
                        modified
                    )(cursor)
                else:
                    part.store_copy_from(
                        data_package,
                        modified
                    )(cursor)

                part.mark_modified(
                    data_package.timestamp,
                    modified
                )(cursor)

        return f

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


def create_copy_from_lines(timestamp, modified, rows, serializers):
    map_values = zip_apply(serializers)

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
