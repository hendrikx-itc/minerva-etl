# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import os
import re
import logging
from contextlib import closing
import psycopg2.extras
from functools import wraps

from minerva.db import parse_db_url
from minerva.directory.helpers_v4 import create_entitytype, get_entitytype, \
		create_entity, get_entity, none_or, get_entitytype_by_id, get_datasource, \
		create_datasource


def connect():
	db_url = os.getenv("TEST_DB_URL")

	if db_url is None:
		raise Exception("Environment variable TEST_DB_URL not set")

	scheme, user, password, host, port, database = parse_db_url(db_url)

	if scheme != "postgresql":
		raise Exception("Only PostgreSQL connections are supported")

	conn = psycopg2.connect(database=database, user=user, password=password,
		 host=host, port=port, connection_factory=psycopg2.extras.LoggingConnection)

	logging.info("connected to database {0}/{1}".format(host, database))

	conn.initialize(logging.getLogger(""))

	return conn


def with_connection(factory_fn=connect):
	def decorator_fn(f):
		@wraps(f)
		def wrapper(*args, **kwargs):
			with closing(factory_fn()) as conn:
				return f(conn, *args, **kwargs)

		return wrapper

	return decorator_fn


def clear_database(cursor):
	cursor.execute("DELETE FROM notification.notificationstore CASCADE")


def get_or_create_datasource(cursor, name):
	datasource = get_datasource(cursor, name)

	if not datasource:
		datasource = create_datasource(cursor, name, "Dummy source for integration test", "UTC", "event")

	return datasource


def get_or_create_entitytype(cursor, name):
	entitytype = get_entitytype(cursor, name)

	if not entitytype:
		entitytype = create_entitytype(cursor, name, "")

	return entitytype
