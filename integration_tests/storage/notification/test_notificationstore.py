# -*- coding: utf-8 -*-
from datetime import datetime
from contextlib import closing

from minerva.directory import DataSource
from minerva.directory.entityref import EntityIdRef
from minerva.storage import datatype
from minerva.storage.notification import NotificationStore, \
    Record, NotificationStoreDescriptor, AttributeDescriptor
from minerva.test import clear_database


def test_create(start_db_container):
    conn = clear_database(start_db_container)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-source-001")(cursor)

        attribute_descriptors = [
            AttributeDescriptor('x', datatype.registry['integer'], '')
        ]

        notification_store = NotificationStore.create(
            NotificationStoreDescriptor(
                data_source, attribute_descriptors
            )
        )(cursor)

        assert notification_store.id is not None

        query = (
            "SELECT data_source_id "
            "FROM notification_directory.notification_store "
            "WHERE id = %s"
        )

        args = (notification_store.id,)

        cursor.execute(query, args)

        assert cursor.rowcount == 1

        data_source_id, = cursor.fetchone()

        assert data_source_id == data_source.id


def test_store(start_db_container):
    conn = clear_database(start_db_container)

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-source-002")(cursor)

        attribute_descriptors = [
            AttributeDescriptor("a", datatype.registry[
                'integer'], "a attribute"),
            AttributeDescriptor("b", datatype.registry[
                'integer'], "b attribute")
        ]

        notification_store = NotificationStore.create(
            NotificationStoreDescriptor(
                data_source, attribute_descriptors
            )
        )(cursor)

        record = Record(
            entity_ref=EntityIdRef(100),
            timestamp=datetime(2013, 6, 5, 12, 0, 0),
            attribute_names=["a", "b"],
            values=[1, 42]
        )

        notification_store.store_record(record)(cursor)
