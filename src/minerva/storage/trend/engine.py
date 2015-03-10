from contextlib import closing

from minerva.directory import EntityType
from minerva.storage import Engine
from minerva.storage.trend import TableTrendStore


class TrendEngine(Engine):
    @staticmethod
    def store(package):
        """
        Return a function to bind a data source to the store command.

        :param package: A DataPackageBase subclass instance
        :return: function that can bind a data source to the store command
        :rtype: (data_source) -> (conn) -> None
        """
        def bind_data_source(data_source):
            def execute(conn):
                entity_type_name = package.entity_type_name()

                with closing(conn.cursor()) as cursor:
                    entity_type = EntityType.get_by_name(entity_type_name)(
                        cursor
                    )

                    trend_store = TableTrendStore.get(
                        data_source, entity_type, package.granularity
                    )(cursor)

                trend_store.store(package).run(conn)

            return execute

        return bind_data_source