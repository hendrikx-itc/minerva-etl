# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from datetime import datetime
from contextlib import closing

from nose.tools import eq_

from minerva.directory import DataSource
from minerva.storage.notification.types import NotificationStore, Attribute, \
    Record
from minerva.test import with_conn

from minerva_db import clear_database


@with_conn(clear_database)
def test_create(conn):
    with closing(conn.cursor()) as cursor:
        datasource = DataSource.from_name(cursor, "test-source-001")

        notificationstore = NotificationStore(datasource, [])

        notificationstore.create(cursor)

        assert notificationstore.id is not None

        query = (
            "SELECT datasource_id "
            "FROM notification.notificationstore "
            "WHERE id = %s"
        )

        args = (notificationstore.id,)

        cursor.execute(query, args)

        eq_(cursor.rowcount, 1)

        datasource_id, = cursor.fetchone()

        eq_(datasource_id, datasource.id)


@with_conn(clear_database)
def test_store(conn):
    with closing(conn.cursor()) as cursor:
        datasource = DataSource.from_name(cursor, "test-source-002")

        attributes = [
            Attribute("a", "integer", "a attribute"),
            Attribute("b", "integer", "b attribute")
        ]

        notificationstore = NotificationStore(datasource, attributes)

        notificationstore.create(cursor)

        datarecord = Record(
            entity_id=100,
            timestamp=datetime(2013, 6, 5, 12, 0, 0),
            attribute_names=["a", "b"],
            values=[1, 42]
        )

        notificationstore.store_record(datarecord)(cursor)
