# -*- coding: utf-8 -*-
from datetime import datetime
from contextlib import closing
from itertools import chain
from typing import List, Callable, Any, Iterable, Generator

import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extensions import adapt, register_adapter, AsIs, QuotedString

from minerva.storage.trend.datapackage import DataPackageRow
from minerva.db import CursorDbAction, ConnDbAction
from minerva.db.util import create_file
from minerva.db.error import DuplicateTable
from minerva.storage import datatype, DataPackage
from minerva.db.query import Table
from minerva.storage.trend import schema
from minerva.storage.trend.trend import Trend, NoSuchTrendError
from minerva.db.error import (
    translate_postgresql_exception,
    translate_postgresql_exceptions,
    DataTypeMismatch,
    UniqueViolation,
)
from minerva.util import zip_apply, first

LARGE_BATCH_THRESHOLD = 10


class PartitionExistsError(Exception):
    def __init__(self, trend_store_part_id, partition_index):
        self.trend_store_part_id = trend_store_part_id
        self.partition_index = partition_index


class TrendStorePart:
    id_: int
    name: str
    trends: List[Trend]

    class Descriptor:
        name: str
        trend_descriptors: List[Trend.Descriptor]
        generated_trend_descriptors: List[Trend.Descriptor]

        def __init__(self, name: str, trend_descriptors: List[Trend.Descriptor]):
            self.name = name
            self.trend_descriptors = trend_descriptors
            self.generated_trend_descriptors = []

    def __init__(self, id_: int, trend_store, name: str, trends: List[Trend]):
        self.id = id_
        self.trend_store = trend_store
        self.name = name
        self.trends = trends

    def __str__(self):
        return self.base_table_name()

    @staticmethod
    def get_trends(cursor, trend_store_part_id) -> List[Trend]:
        query = (
            "SELECT id, name, data_type, trend_store_part_id, description "
            "FROM trend_directory.table_trend "
            "WHERE trend_store_part_id = %s"
        )

        args = (trend_store_part_id,)

        cursor.execute(query, args)

        return [
            Trend(id_, name, datatype.registry[data_type], trend_store_id, description)
            for id_, name, data_type, trend_store_id, description in cursor.fetchall()
        ]

    @staticmethod
    def from_record(record, trend_store: "TrendStore") -> Callable[[Any], Any]:
        """
        Return function that can instantiate a TrendStore from a
        trend_store type record.
        :param trend_store:
        :param record: An iterable that represents a trend_store record
        :return: function that creates and returns TrendStore object
        """

        def f(cursor):
            (trend_store_part_id, trend_store_id, name) = record

            trends = TrendStorePart.get_trends(cursor, trend_store_part_id)

            return TrendStorePart(trend_store_part_id, trend_store, name, trends)

        return f

    def base_table_name(self) -> str:
        """
        Return the base/parent table name.

        :return: table name
        """
        return self.name

    def base_table(self) -> Table:
        return Table("trend", self.base_table_name())

    def get_copy_serializers(self, trend_names: Iterable[str]):
        trend_by_name = {t.name: t for t in self.trends}

        def get_serializer_by_trend_name(name):
            try:
                trend = trend_by_name[name]
            except KeyError:
                raise NoSuchTrendError(f"no trend with name {name}")
            else:
                data_type = trend.data_type

                return data_type.string_serializer(
                    datatype.copy_from_serializer_config(data_type)
                )

        return [get_serializer_by_trend_name(name) for name in trend_names]

    @classmethod
    def get_by_id(cls, trend_store_part_id: int) -> CursorDbAction:
        def f(cursor):
            args = (trend_store_part_id,)

            cls.get_by_id_query.execute(cursor, args)

            if cursor.rowcount == 1:
                return TrendStorePart.from_record(cursor.fetchone())(
                    cursor
                )

            return None

        return f

    def check_trends_exist(
        self, trend_descriptors: List[Trend.Descriptor]
    ) -> CursorDbAction:
        """
        Returns function that creates missing trends as described by
        'trend_descriptors' and returns a new TrendStore.

        :param trend_descriptors: A list with trend descriptors indicating the
        required trends and their data types.
        :return:
        """
        query = (
            "SELECT trend_directory.assure_table_trends_exist("
            "trend_store, %s::trend_directory.trend_descr[]"
            ") "
            "FROM trend_directory.trend_store "
            "WHERE id = %s"
        )

        args = trend_descriptors, self.id

        def f(cursor):
            cursor.execute(query, args)

            return TrendStorePart.get_by_id(self.id)(cursor)

        return f

    def store(self, data_package: DataPackage, job_id: int) -> ConnDbAction:
        def f(conn):
            try:
                with closing(conn.cursor()) as cursor:
                    modified = get_timestamp(cursor)

                    self.store_copy_from(data_package, modified, job_id)(cursor)

                    for timestamp in data_package.timestamps():
                        self.mark_modified(timestamp, modified)(cursor)

            except DataTypeMismatch as exc:
                conn.rollback()

                raise exc

            except UniqueViolation:
                # Try again through a slower but more reliable method
                conn.rollback()

                with closing(conn.cursor()) as cursor:
                    modified = get_timestamp(cursor)

                    self.securely_store_copy_from(data_package, modified, job_id)(
                        cursor
                    )

                    for timestamp in data_package.timestamps():
                        self.mark_modified(timestamp, modified)(cursor)

            conn.commit()

        return f

    def store_copy_from(
        self, data_package: DataPackage, modified: datetime, job_id: int
    ) -> CursorDbAction:
        """
        Store the data using the PostgreSQL specific COPY FROM command
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
                modified, job_id, data_package.refined_rows(cursor), serializers
            )

            copy_from_query = create_copy_from_query(self.base_table(), trend_names)
            
            try:
                cursor.copy_expert(copy_from_query, copy_from_file)
            except psycopg2.DatabaseError as exc:
                raise translate_postgresql_exception(exc)

            # Only valid for psycopg <2.8, so not compatible with the version on Ubuntu 18.04
            # try:
            #    cursor.copy_expert(copy_from_query, copy_from_file)
            # except psycopg2.errors.InvalidTextRepresentation as exc:
            #    raise DataTypeMismatch()

        return f

    def securely_store_copy_from(
        self, data_package: DataPackage, modified: datetime, job_id: int
    ) -> CursorDbAction:
        """
        Same function as the previous, but with a slower, but less error-prone
        method
        """

        def f(cursor):
            trend_names = [
                trend_descriptor.name
                for trend_descriptor in data_package.trend_descriptors
            ]

            column_names = list(chain(schema.system_columns, trend_names))

            values = [
                create_value_row(modified, job_id, row)
                for row in data_package.refined_rows(cursor)
            ]

            command = create_insert_query(self.base_table(), column_names)

            try:
                psycopg2.extras.execute_batch(cursor, command, values)
            except psycopg2.DatabaseError as exc:
                raise translate_postgresql_exception(exc)

        return f

    @staticmethod
    def _update_existing_from_tmp(
        tmp_table: Table, table: Table, column_names: List[str], modified: datetime
    ) -> CursorDbAction:
        def f(cursor):
            set_columns = sql.SQL(", ").join(
                sql.SQL("{0}={1}.{0}").format(
                    sql.Identifier(name), tmp_table.identifier()
                )
                for name in column_names
            )

            update_query = sql.SQL(
                "UPDATE {0} SET modified=greatest(%s, {0}.modified), {1} "
                "FROM {2} "
                "WHERE {0}.entity_id={2}.entity_id "
                'AND {0}."timestamp"={2}."timestamp"'
            ).format(table.identifier(), set_columns, tmp_table.identifier())

            args = (modified,)

            try:
                cursor.execute(update_query, args)
            except psycopg2.DatabaseError as exc:
                raise translate_postgresql_exception(exc)

        return f

    @staticmethod
    def _copy_missing_from_tmp(
        tmp_table: Table, table: Table, column_names: List[str]
    ) -> CursorDbAction:
        """
        Store the data using the PostgreSQL specific COPY FROM command and a
        temporary table. The temporary table is joined against the target table
        to make sure only missing records (based on entity_id, timestamp
        combination) are inserted.
        """

        def f(cursor):
            all_column_names = ["entity_id", "timestamp", "modified"]
            all_column_names.extend(column_names)

            tmp_column_names = sql.SQL(", ").join(
                sql.Identifier("tmp", name) for name in all_column_names
            )

            dest_column_names = sql.SQL(", ").join(
                sql.Identifier(name) for name in all_column_names
            )

            insert_query = sql.SQL(
                "INSERT INTO {table} ({dest_columns}) "
                "SELECT {tmp_columns} FROM {tmp_table} AS tmp "
                "LEFT JOIN {table} ON "
                'tmp."timestamp" = {table}."timestamp" '
                "AND tmp.entity_id = {table}.entity_id "
                "WHERE {table}.entity_id IS NULL"
            ).format(
                table=table.identifier(),
                dest_columns=dest_column_names,
                tmp_columns=tmp_column_names,
                tmp_table=tmp_table.identifier(),
            )

            try:
                cursor.execute(insert_query)
            except psycopg2.Error as exc:
                raise translate_postgresql_exception(exc)

        return f

    @translate_postgresql_exceptions
    def mark_modified(self, timestamp: datetime, modified: datetime) -> CursorDbAction:
        def f(cursor):
            args = self.id, timestamp, modified

            cursor.callproc("trend_directory.mark_modified", args)

        return f

    def ensure_data_types(
        self, trend_descriptors: List[Trend.Descriptor]
    ) -> CursorDbAction:
        """
        Check if database column types match trend data type and correct it if
        necessary.

        :param trend_descriptors: A list with trend descriptors indicating the
        required data type of the corresponding trends.
        """
        query = (
            "SELECT trend_directory.assure_data_types("
            "trend_store_part, %s::trend_directory.trend_descr[]"
            ") "
            "FROM trend_directory.trend_store_part "
            "WHERE id = %s"
        )

        args = trend_descriptors, self.id

        def f(cursor):
            cursor.execute(query, args)

        return f

    def create_partition(self, conn, partition_index: int):
        query = (
            "SELECT p.name, trend_directory.create_partition(p.id, %s) "
            "FROM trend_directory.trend_store_part p "
            "WHERE p.id = %s"
        )
        args = (partition_index, self.id)

        with closing(conn.cursor()) as cursor:
            try:
                cursor.execute(query, args)
            except DuplicateTable:
                raise PartitionExistsError(self.id, partition_index)

            name, _ = cursor.fetchone()

            return name


def adapt_trend_store_part(trend_store_part: TrendStorePart.Descriptor):
    """Return psycopg2 compatible representation of `trend_store_part`."""
    return AsIs(
        "({}, {}::trend_directory.trend_descr[], {}::trend_directory.generated_trend_descr[])".format(
            QuotedString(trend_store_part.name),
            adapt(trend_store_part.trend_descriptors),
            adapt(trend_store_part.generated_trend_descriptors),
        )
    )


register_adapter(TrendStorePart.Descriptor, adapt_trend_store_part)


def create_copy_from_query(table: Table, trend_names: List[str]) -> str:
    """Return SQL query that can be used in the COPY FROM command."""
    column_names = chain(schema.system_columns, trend_names)

    return sql.SQL("COPY {}({}) FROM STDIN").format(
        table.identifier(), sql.SQL(",").join([sql.Identifier(column_name) for column_name in column_names])
    )


def create_insert_query(table: Table, column_names: List[str]) -> sql.SQL:
    """Return insertion query to be performed when copy fails."""
    update_parts = [
        sql.SQL("{} = {}").format(
            sql.Identifier(column), sql.Identifier("excluded", column)
        )
        for column in column_names
        if column not in ("entity_id", "timestamp", "created")
    ]

    return sql.SQL(
        "INSERT INTO {0}({1}) "
        "VALUES ({2}) "
        "ON CONFLICT (entity_id, timestamp) DO UPDATE SET {3};"
    ).format(
        table.identifier(),
        sql.SQL(",").join(sql.Identifier(column_name) for column_name in column_names),
        sql.SQL(",").join(sql.Placeholder() for _ in column_names),
        sql.SQL(",").join(update_parts),
    )


def create_copy_from_lines(
    modified: datetime, job: int, rows: List[DataPackageRow], serializers: List
) -> Generator[str, None, None]:
    """Return a generator with all the lines for a copy-from file."""
    map_values = zip_apply(serializers)

    job_id = job or "NULL"

    def to_values_part(values):
        return "\t".join(map_values(values))

    return (
        f"{entity_id}\t'{timestamp.isoformat()}'\t'{modified.isoformat()}'\t{job_id}\t{to_values_part(values)}\n"
        for entity_id, timestamp, values in rows
    )


def create_value_row(modified: datetime, job_id: int, row: DataPackageRow):
    """Create a flat list from the provided data."""
    (entity_id, timestamp, values) = row
    return [str(entity_id), timestamp, modified, job_id] + list(values)


def create_copy_from_file(
    modified: datetime, job_id: int, rows: List[DataPackageRow], serializers: List
):
    """Create an in memory file to use in copy-from command."""
    return create_file(create_copy_from_lines(modified, job_id, rows, serializers))


def get_timestamp(cursor) -> datetime:
    """Return the current timestamp from the database."""
    cursor.execute("SELECT NOW()")

    return first(cursor.fetchone())
