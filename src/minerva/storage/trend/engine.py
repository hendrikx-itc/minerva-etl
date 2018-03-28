from contextlib import closing
from operator import contains
from functools import partial

from minerva.util import k, identity
from minerva.directory import EntityType, NoSuchEntityType
from minerva.storage import Engine
from minerva.storage.trend.tabletrendstore import TableTrendStore, \
    NoSuchTableTrendStore
from minerva.storage.trend.datapackage import DataPackage


class TrendEngine(Engine):
    pass_through = k(identity)

    @staticmethod
    def store_cmd(package: DataPackage):
        """
        Return a function to bind a data source to the store command.

        :param package: A DataPackageBase subclass instance
        :return: function that binds a data source to the store command
        :rtype: (data_source) -> (conn) -> None
        """
        return TrendEngine.make_store_cmd(TrendEngine.pass_through)(package)

    @staticmethod
    def make_store_cmd(transform_package):
        """
        Return a function to bind a data source to the store command.

        :param transform_package: (TableTrendStore) -> (DataPackage)
        -> DataPackage
        """
        def cmd(package: DataPackage):
            def bind_data_source(data_source):
                def execute(conn):
                    trend_store = trend_store_for_package(
                        data_source, package
                    )(conn)

                    verify_partition_for_package(trend_store, package)(conn)

                    trend_store.store(
                        transform_package(trend_store)(package)
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


def trend_store_for_package(data_source, package: DataPackage):
    def f(conn):
        entity_type_name = package.entity_type_name()

        with closing(conn.cursor()) as cursor:
            entity_type = EntityType.get_by_name(entity_type_name)(cursor)

            if entity_type is None:
                raise NoSuchEntityType(entity_type_name)
            else:
                table_trend_store = TableTrendStore.get(
                    data_source, entity_type, package.granularity
                )(cursor)

                if table_trend_store is None:
                    raise NoSuchTableTrendStore(
                        data_source, entity_type, package.granularity
                    )

                return table_trend_store

    return f


def verify_partition_for_package(
        trend_store: TableTrendStore, package: DataPackage):
    def f(conn):
        with closing(conn.cursor()) as cursor:
            parts = {
                trend_store._trend_part_mapping[trend_name]
                for trend_name in package.trend_descriptors
                if trend_name in trend_store._trend_part_mapping
            }

            for part in parts:
                partition = part.partition(package.timestamp)

                if not partition.exists(cursor):
                    partition.create(cursor)

    return f
