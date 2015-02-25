# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
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
        AttributeDescriptor("a", datatype.Integer, "a attribute"),
        AttributeDescriptor("b", datatype.Integer, "b attribute")
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
