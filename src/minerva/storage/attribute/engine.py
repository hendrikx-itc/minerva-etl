from contextlib import closing

from minerva.directory import EntityType

from minerva.storage import Engine
from minerva.storage.attribute.attributestore import AttributeStore


class AttributeEngine(Engine):
    @staticmethod
    def store_cmd(package):
        def bind_data_source(data_source):
            def execute(conn):
                entity_type_name = package.entity_type_name()

                with closing(conn.cursor()) as cursor:
                    entity_type = EntityType.get_by_name(entity_type_name)(
                        cursor
                    )

                attribute_store = AttributeStore.get_by_attributes(
                    data_source, entity_type
                )(cursor)

                attribute_store.store(package)(conn)

            return execute

        return bind_data_source
