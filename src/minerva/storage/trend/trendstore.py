# -*- coding: utf-8 -*-
from minerva.db.util import quote_ident
from minerva.db.query import Table, Column, Eq, ands
from minerva.directory import DataSource, EntityType
from minerva.storage.trend import schema
from minerva.storage.trend.granularity import Granularity
from minerva.storage.trend.trend import Trend
from minerva.storage import datatype
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage.trend.tables import DATA_TABLE_POSTFIXES


class NoSuchTrendError(Exception):
    pass


class TimestampEquals:
    def __init__(self, timestamp):
        self.timestamp = timestamp

    def render(self):
        return 'timestamp = %s', (self.timestamp,)


class TrendStoreQuery:
    def __init__(self, trend_store, trend_names):
        self.trend_store = trend_store
        self.trend_names = trend_names
        self.timestamp_constraint = None

    def execute(self, cursor):
        args = tuple()

        query = (
            'SELECT {} FROM {}'
        ).format(
            ', '.join(map(quote_ident, self.trend_names)),
            self.trend_store.table().render()
        )

        if self.timestamp_constraint is not None:
            query_part, args_part = self.timestamp_constraint.render()

            query += ' WHERE {}'.format(query_part)
            args += args_part

        cursor.execute(query, args)

        return cursor

    def timestamp(self, constraint):
        self.timestamp_constraint = constraint

        return self


class TrendStore:
    class Descriptor:
        def __init__(
                self, data_source: DataSource, entity_type: EntityType,
                granularity: Granularity):
            self.data_source = data_source
            self.entity_type = entity_type
            self.granularity = granularity

    """
    All data belonging to a specific data source, entity type and granularity.
    """
    column_names = [
        "id", "data_source_id", "entity_type_id", "granularity",
        "partition_size"
    ]

    columns = list(map(Column, column_names))

    get_query = schema.trend_store.select(columns).where_(ands([
        Eq(Column("data_source_id")),
        Eq(Column("entity_type_id")),
        Eq(Column("granularity"))
    ]))

    get_by_id_query = schema.trend_store.select(
        columns
    ).where_(Eq(Column("id")))

    def __init__(
            self, id_, data_source, entity_type, granularity):
        self.id = id_
        self.data_source = data_source
        self.entity_type = entity_type
        self.granularity = granularity

    def get_trend(self, cursor, trend_name):
        query = (
            "SELECT id, name, data_type, trend_store_id, description "
            "FROM trend_directory.trend "
            "WHERE trend_store_id = %s AND name = %s"
        )

        args = self.id, trend_name

        cursor.execute(query, args)

        if cursor.rowcount > 0:
            return Trend(*cursor.fetchone())

    def get_value_descriptors(self, trend_names):
        trend_by_name = {t.name: t for t in self.trends}

        def get_descriptor_by_trend_name(name):
            try:
                trend = trend_by_name[name]
            except KeyError:
                raise NoSuchTrendError('no trend with name {}'.format(name))
            else:
                data_type = trend.data_type

                return ValueDescriptor(name, data_type)

        return [
            get_descriptor_by_trend_name(name)
            for name in trend_names
        ]

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

    def retrieve(self, trend_names):
        return TrendStoreQuery(self, trend_names)
