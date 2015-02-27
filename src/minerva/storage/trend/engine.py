from contextlib import closing

from minerva.directory import EntityType
from minerva.storage import Engine
from minerva.storage.trend import TableTrendStore


class TrendEngine(Engine):
    @staticmethod
    def store(package):
        def bind_data_source(data_source):
            def execute(conn):
                entity_type_name = package.entity_type_name()

                with closing(conn.cursor()) as cursor:
                    entity_type = EntityType.from_name(entity_type_name)(
                        cursor
                    )

                    trend_store = TableTrendStore.get(
                        data_source, entity_type, package.granularity
                    )(cursor)

                trend_store.store(package).run(conn)

            return execute

        return bind_data_source