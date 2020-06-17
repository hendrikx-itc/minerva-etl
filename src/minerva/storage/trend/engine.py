from contextlib import closing
from operator import contains
from functools import partial
from typing import Callable

from psycopg2.extensions import connection

from minerva.util import k, identity
from minerva.directory import EntityType, NoSuchEntityType, DataSource
from minerva.storage import Engine
from minerva.storage.trend.trendstore import TrendStore, \
    NoSuchTrendStore
from minerva.storage.trend.datapackage import DataPackage


class TrendEngine(Engine):
    pass_through = k(identity)

    @staticmethod
    def store_cmd(package: DataPackage, description: dict):
        """
        Return a function to bind a data source to the store command.

        :param package: A DataPackageBase subclass instance
        :param description: A description of the task that generated the data package
        :return: function that binds a data source to the store command
        :rtype: (data_source) -> (conn) -> None
        """
        return TrendEngine.make_store_cmd(TrendEngine.pass_through)(package, description)

    @staticmethod
    def make_store_cmd(transform_package) -> Callable[[DataPackage, dict], Callable[[DataSource], Callable[[connection], None]]]:
        """
        Return a function to bind a data source to the store command.

        :param transform_package: (TrendStore) -> (DataPackage)
        -> DataPackage
        """
        def cmd(package: DataPackage, description: dict):
            def bind_data_source(data_source: DataSource):
                def execute(conn):
                    trend_store = trend_store_for_package(
                        data_source, package
                    )(conn)

                    trend_store.store(
                        transform_package(trend_store)(package),
                        description
                    )(conn)

                    conn.commit()

                return execute

            return bind_data_source

        return cmd

    @staticmethod
    def filter_existing_trends(trend_store):
        """
        Return function that transforms a data package to only contain trends
        that are defined by *trend_store*.

        :param trend_store: trend store with defined trends
        :return: (DataPackage) -> DataPackage
        """
        def f(package):
            return package.filter_trends(
                partial(contains, trend_store._trend_part_mapping)
            )

        return f


def trend_store_for_package(data_source: DataSource, package: DataPackage):
    def f(conn) -> TrendStore:
        entity_type_name = package.entity_type_name()

        with closing(conn.cursor()) as cursor:
            entity_type = EntityType.get_by_name(entity_type_name)(cursor)

            if entity_type is None:
                raise NoSuchEntityType(entity_type_name)
            else:
                table_trend_store = TrendStore.get(
                    data_source, entity_type, package.granularity
                )(cursor)

                if table_trend_store is None:
                    raise NoSuchTrendStore(
                        data_source, entity_type, package.granularity
                    )

                return table_trend_store

    return f
