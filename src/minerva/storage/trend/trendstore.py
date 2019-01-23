# -*- coding: utf-8 -*-
from minerva.db.query import Column, Eq, ands
from minerva.directory import DataSource, EntityType
from minerva.storage.trend import schema
from minerva.storage.trend.granularity import Granularity
from minerva.storage.trend.trend import Trend


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
            "SELECT trend.id, trend.name, data_type, trend_store_part_id, description "
            "FROM trend_directory.trend "
            "JOIN trend_directory.trend_store_part "
            "ON trend_store_part_id = trend_store_part.id "
            "WHERE trend_store_id = %s AND trend.name = %s"
        )

        args = self.id, trend_name

        cursor.execute(query, args)

        if cursor.rowcount > 0:
            return Trend(*cursor.fetchone())
