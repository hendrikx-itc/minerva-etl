# -*- coding: utf-8 -*-
from contextlib import closing
from typing import List, Callable, Any
from datetime import datetime

from minerva.db.query import Column, Eq, ands
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.partitioning import Partitioning

from minerva.storage.trend import schema
from minerva.directory import DataSource, EntityType
from minerva.storage.trend.granularity import create_granularity, Granularity
from minerva.storage.trend.tabletrendstorepart import TableTrendStorePart
from minerva.util import string_fns


class NoSuchTableTrendStore(Exception):
    def __init__(self, data_source, entity_type, granularity):
        self.data_source = data_source
        self.entity_type = entity_type
        self.granularity = granularity


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
        self.part_by_name = None
        self._trend_part_mapping = None

    def partition(self, part_name, timestamp: datetime):
        return self.part_by_name[part_name].partition(timestamp)

    def create_partitions(self, timestamp: datetime):
        def f(cursor):
            partitions = [part.partition(timestamp) for part in self.parts]

            for partition in partitions:
                partition.create(cursor)

        return f

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

        self.part_by_name = {
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
        def f(conn):
            args = (id_,)

            with closing(conn.cursor()) as cursor:
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
        return string_fns([
            part.store(package_part)
            for part, package_part in self.split_package_by_parts(data_package)
        ])

    def split_package_by_parts(self, data_package):
        def group_fn(trend_name):
            return self._trend_part_mapping[trend_name].name

        return [
            (self.part_by_name[part_name], package)
            for part_name, package in data_package.split(group_fn)
        ]

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
