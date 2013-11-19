# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing
from datetime import datetime

from minerva.test import with_conn
from minerva.directory.helpers_v4 import name_to_datasource
from minerva.storage.notification.types import NotificationStore, Attribute, Record

from minerva_db import clear_database


@with_conn(clear_database)
def test_store_record(conn):
    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, "test-source-003")

        attributes = [
            Attribute("a", "integer", "a attribute"),
            Attribute("b", "integer", "b attribute")]

        notificationstore = NotificationStore(datasource, attributes)

        notificationstore.create(cursor)

        datarecord = Record(
            entity_id=100,
            timestamp=datetime(2013, 6, 5, 12, 0, 0),
            attribute_names=["a", "b"],
            values=[1, 42])

        notificationstore.store_record(datarecord)(cursor)
