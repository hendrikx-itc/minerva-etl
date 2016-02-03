# -*- coding: utf-8 -*-
from contextlib import closing
from datetime import datetime

from minerva.test import with_conn, clear_database
from minerva.directory import DataSource
from minerva.directory.entityref import EntityIdRef
from minerva.storage import datatype
from minerva.storage.notification import NotificationStore, \
    NotificationStoreDescriptor, AttributeDescriptor, Record


@with_conn(clear_database)
def test_store_record(conn):
    attribute_descriptors = [
        AttributeDescriptor("a", datatype.registry['integer'], "a attribute"),
        AttributeDescriptor("b", datatype.registry['integer'], "b attribute")
    ]

    with closing(conn.cursor()) as cursor:
        data_source = DataSource.from_name("test-source-003")(cursor)

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
