# -*- coding: utf-8 -*-
import logging
from contextlib import closing
from itertools import chain
from typing import List, Callable, Any
from datetime import datetime

import psycopg2

from minerva.db.util import create_temp_table_from, quote_ident, create_file
from minerva.storage import datatype
from minerva.db.query import Table
from minerva.storage.trend import schema
from minerva.storage.trend.trend import Trend, NoSuchTrendError
from minerva.storage.trend.partition import Partition

from minerva.db.error import NoCopyInProgress, \
    translate_postgresql_exception, translate_postgresql_exceptions
from minerva.util import compose, zip_apply, first

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

    def __init__(
            self, id_: int, table_trend_store, name: str, trends: List[Trend]):
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

    def store(self, data_package):
        def f(conn):
            try:
                with closing(conn.cursor()) as cursor:
                    modified = get_timestamp(cursor)

                    self.store_copy_from(data_package, modified)(cursor)

            except psycopg2.DatabaseError as exc:
                if exc.pgcode is None and str(exc).find(
                        "no COPY in progress") != -1:
                    # Might happen after database connection loss
                    raise NoCopyInProgress()
                else:
                    # Try again through a slower but more reliable method
                    with closing(conn.cursor()) as cursor:
                        cursor.execute('rollback')
                        modified = get_timestamp(cursor)
                        self.securely_store_copy_from(data_package, modified)(cursor)

        return f

    def store_copy_from(self, data_package, modified):
        """
        Store the data using the PostgreSQL specific COPY FROM command

        :param data_package: A DataPackage object
        """

        def f(cursor):
            trend_names = [
                trend_descriptor.name
                for trend_descriptor in data_package.trend_descriptors
            ]

            serializers = self.get_copy_serializers(
                trend_descriptor.name
                for trend_descriptor in data_package.trend_descriptors
            )

            copy_from_file = create_copy_from_file(
                modified,
                data_package.refined_rows(cursor),
                serializers
            )

            copy_from_query = create_copy_from_query(
                self.base_table(), trend_names
            )

            cursor.copy_expert(copy_from_query, copy_from_file)

        return f


    def securely_store_copy_from(self, data_package, modified):
        """
        Same function as the previous, but with a slower, but less error-prone method
        """
        def f(cursor):
            trend_names = [
                trend_descriptor.name
                for trend_descriptor in data_package.trend_descriptors
            ]

            serializers = self.get_copy_serializers(
                trend_descriptor.name
                for trend_descriptor in data_package.trend_descriptors
            )

            copy_from_file = create_copy_from_file(
                modified,
                data_package.refined_rows(cursor),
                serializers
            )

            column_names = list(chain(schema.system_columns, trend_names))

            try:
                for line in copy_from_file.readlines():
                    command = create_insertion_command(
                        self.base_table(), column_names, line.strip().split('\t')
                    )
                    cursor.execute(command)

            except psycopg2.DatabaseError as exc:
                raise translate_postgresql_exception(exc)

        return f


    def store_update(self, data_package, modified):
        def f(cursor):
            table = self.base_table()

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


def create_copy_from_query(table, trend_names):
    """Return SQL query that can be used in the COPY FROM command."""
    column_names = chain(schema.system_columns, trend_names)

    return "COPY {0}({1}) FROM STDIN".format(
        table.render(),
        ",".join(map(quote_ident, column_names))
    )

def create_insertion_command(table, column_names, values):
    """Return insertion query to be performed when copy fails"""
    return 'INSERT INTO trend."{0}"({1}) VALUES({2}) ON CONFLICT ON CONSTRAINT airbox_pkey DO UPDATE SET {3};'.format(
        table.name,
        ",".join(map(quote_ident, column_names)),
        ",".join(values),
        ", ".join(create_update_command_parts(column_names, values))
    )

def create_update_command_parts(columns, values):
    return [
        '"{0}" = {1}'.format(pair[0], pair[1]) for pair in zip(columns, values) if pair[0] != 'created'
    ]

def create_copy_from_lines(modified, rows, serializers):
    map_values = zip_apply(serializers)

    return (
        u"{0:d}\t'{1!s}'\t'{2!s}'\t'{2!s}'\t{3}\n".format(
            entity_id,
            timestamp.isoformat(),
            modified.isoformat(),
            "\t".join(map_values(values))
        )
        for entity_id, timestamp, values in rows
    )


create_copy_from_file = compose(create_file, create_copy_from_lines)


def get_timestamp(cursor):
    cursor.execute("SELECT NOW()")

    return first(cursor.fetchone())
