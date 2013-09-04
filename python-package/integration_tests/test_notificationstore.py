# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import time
from contextlib import closing
from pytz import timezone

from nose.tools import eq_, raises, assert_not_equal

from minerva.storage.generic import extract_data_types
from datetime import datetime, timedelta
from minerva_storage_notification.types import NotificationStore, Attribute, Record
from minerva_db import with_connection, clear_database, \
		get_or_create_datasource, get_or_create_entitytype


@with_connection()
def test_create(conn):
	with closing(conn.cursor()) as cursor:
		clear_database(cursor)

		datasource = get_or_create_datasource(cursor, "test-source-001")
		entitytype = get_or_create_entitytype(cursor, "node")

		notificationstore = NotificationStore(datasource, entitytype, [])

		notificationstore.create(cursor)

		assert notificationstore.id is not None

		query = (
			"SELECT datasource_id, entitytype_id "
			"FROM notification.notificationstore "
			"WHERE id = %s")

		args = (notificationstore.id,)

		cursor.execute(query, args)

		eq_(cursor.rowcount, 1)

		datasource_id, entitytype_id = cursor.fetchone()

		eq_(datasource_id, datasource.id)
		eq_(entitytype_id, entitytype.id)


@with_connection()
def test_store(conn):
	with closing(conn.cursor()) as cursor:
		clear_database(cursor)

		datasource = get_or_create_datasource(cursor, "test-source-002")
		entitytype = get_or_create_entitytype(cursor, "node")

		attributes = [
			Attribute("a", "integer", "a attribute"),
			Attribute("b", "integer", "b attribute")]

		notificationstore = NotificationStore(datasource, entitytype, attributes)

		notificationstore.create(cursor)

		datarecord = Record(
			entity_id=100,
			timestamp=datetime(2013, 6, 5, 12, 0, 0),
			attribute_names=["a", "b"],
			values=[1, 42])

		notificationstore.store_record(datarecord)(cursor)
