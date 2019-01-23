# -*- coding: utf-8 -*-
from minerva.db.util import quote_ident
from minerva.db.query import Column, Eq, ands
from minerva.storage.trend import schema
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.trend import Trend


class TimestampEquals:
    def __init__(self, timestamp):
        self.timestamp = timestamp

    def render(self):
        return 'timestamp = %s', (self.timestamp,)


class TrendStorePartQuery:
    def __init__(self, trend_store_part, trend_names):
        self.trend_store_part = trend_store_part
        self.trend_names = trend_names
        self.timestamp_constraint = None

    def execute(self, cursor):
        args = tuple()

        query = (
            'SELECT {} FROM {}'
        ).format(
            ', '.join(map(quote_ident, self.trend_names)),
            self.trend_store.part.base_table().render()
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


class TrendStorePart:
    class Descriptor:
        def __init__(
                self, trend_store: TrendStore):
            self.trend_store = trend_store

    """
    All data belonging to a specific data source, entity type and granularity.
    """
    column_names = [
        "id", "trend_store_id", "name"
    ]

    columns = list(map(Column, column_names))

    get_query = schema.trend_store.select(columns).where_(ands([
        Eq(Column("trend_store_id")),
        Eq(Column("name"))
    ]))

    get_by_id_query = schema.trend_store.select(
        columns
    ).where_(Eq(Column("id")))

    def __init__(
            self, id_, trend_store):
        self.id = id_
        self.trend_store = trend_store

    def get_trend(self, cursor, trend_name):
        query = (
            "SELECT id, name, data_type, trend_store_id, description "
            "FROM trend_directory.trend "
            "WHERE trend_store_part_id = %s AND name = %s"
        )

        args = self.id, trend_name

        cursor.execute(query, args)

        if cursor.rowcount > 0:
            return Trend(*cursor.fetchone())

    def retrieve(self, trend_names):
        return TrendStorePartQuery(self, trend_names)
