# -*- coding: utf-8 -*-
from minerva.storage.trend.trendstore import TrendStore
from minerva.directory import DataSource, EntityType
from minerva.storage.trend.granularity import create_granularity


class ViewTrendStoreDescriptor:
    def __init__(
            self, name, data_source, entity_type, granularity, query):
        self.name = name
        self.data_source = data_source
        self.entity_type = entity_type
        self.granularity = granularity
        self.query = query


class ViewTrendStore(TrendStore):
    @staticmethod
    def create(descriptor):
        def f(cursor):
            args = (
                descriptor.name,
                descriptor.data_source.name,
                descriptor.entity_type.name,
                str(descriptor.granularity),
                descriptor.query
            )

            query = (
                "SELECT * FROM trend_directory.create_view_trend_store("
                "%s, %s, %s, %s, %s"
                ")"
            )

            cursor.execute(query, args)

            (
                trend_store_id, name, entity_type_id, data_source_id,
                granularity_str
            ) = cursor.fetchone()

            entity_type = EntityType.get(entity_type_id)(cursor)
            data_source = DataSource.get(data_source_id)(cursor)

            trends = ViewTrendStore.get_trends(cursor, trend_store_id)

            return ViewTrendStore(
                trend_store_id, name, data_source, entity_type,
                create_granularity(granularity_str), trends
            )

        return f
