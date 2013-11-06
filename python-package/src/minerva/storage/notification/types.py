# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2011-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""

from functools import partial

from minerva.util import first
from minerva.db.query import Table
from minerva.db.error import translate_postgresql_exceptions


class Record(object):
    """Wraps all data for one notification."""
    def __init__(self, entity_id, timestamp, attribute_names, values):
        self.entity_id = entity_id
        self.timestamp = timestamp
        self.attribute_names = attribute_names
        self.values = values


class RawRecord(object):
    """Wraps all data for one notification."""
    def __init__(self, dn, timestamp, attribute_names, values):
        self.dn = dn
        self.timestamp = timestamp
        self.attribute_names = attribute_names
        self.values = values


class Package(object):
    def __init__(self, attribute_names, rows):
        self.attribute_names = attribute_names
        self.rows = rows


class Attribute(object):
    """Describes the attribute of a specific NotificationStore."""
    def __init__(self, name, data_type, description):
        self.id = None
        self.notificationstore_id = None
        self.name = name
        self.data_type = data_type
        self.description = description

    def __str__(self):
        return self.name


class NotificationStore(object):
    def __init__(self, datasource, attributes):
        self.id = None
        self.version = 1
        self.datasource = datasource
        self.attributes = attributes
        table_name = datasource.name
        self.table = Table("notification", table_name)

    @staticmethod
    def load(cursor, datasource):
        """Load NotificationStore from database and return it."""
        query = (
            "SELECT id, datasource_id, version "
            "FROM notification.notificationstore "
            "WHERE datasource_id = %s")

        args = datasource.id,

        cursor.execute(query, args)

        if cursor.rowcount == 1:
            id, datasource_id, version = cursor.fetchone()

            return NotificationStore(datasource, [])

    def create(self, cursor):
        """Create notification store in database in return itself."""
        if self.id:
            raise NotImplementedError()
        else:
            query = (
                "INSERT INTO notification.notificationstore "
                "(datasource_id, version) "
                "VALUES (%s, %s) RETURNING id")

            args = self.datasource.id, self.version

            cursor.execute(query, args)

            self.id = first(cursor.fetchone())

            for attribute in self.attributes:
                query = (
                    "INSERT INTO notification.attribute "
                    "(notificationstore_id, name, data_type, description) "
                    "VALUES (%s, %s, %s, %s) "
                    "RETURNING id")

                args = (self.id, attribute.name, attribute.data_type, attribute.description)
                cursor.execute(query, args)

            return self

    def store_record(self, record):
        """Return function that can store the data from a
        :class:`~minerva.storage.notification.types.Record`."""
        @translate_postgresql_exceptions
        def f(cursor):
            quote_column = partial(str.format, '"{}"')
            column_names = ['entity_id', 'timestamp'] + record.attribute_names
            columns_part = ','.join(map(quote_column, column_names))
            num_args = 2 + len(record.attribute_names)
            args_part = ",".join(["%s"] * num_args)

            query = (
                "INSERT INTO {} ({}) "
                "VALUES ({})").format(self.table.render(), columns_part, args_part)

            args = [record.entity_id, record.timestamp] + map(prepare_value, record.values)

            cursor.execute(query, args)

        return f

    def store_rawrecord(self, rawrecord):
        """Return function that can store the data from a :class:`~minerva.storage.notification.types.RawRecord`."""
        @translate_postgresql_exceptions
        def f(cursor):
            column_names = ["entity_id", "\"timestamp\""] + rawrecord.attribute_names
            columns_part = ",".join(column_names)
            num_args = 1 + len(rawrecord.attribute_names)
            args_part = ",".join(["%s"] * num_args)

            query = (
                "INSERT INTO {} ({}) "
                "VALUES ((directory.dn_to_entity(%s)).id, {})").format(self.table.render(), columns_part, args_part)

            args = [rawrecord.dn, rawrecord.timestamp] + rawrecord.values

            cursor.execute(query, args)

        return f

    def store_package(self, package):
        """
        Return function that can store a package with multiple notifications.
        """
        @translate_postgresql_exceptions
        def f(cursor):
            column_names = ["entity_id", "\"timestamp\""] + datarecord.attribute_names
            columns_part = ",".join(column_names)
            num_args = 2 + len(datarecord.attribute_names)
            args_part = ",".join(["%s"] * num_args)

            query = (
                "INSERT INTO {} ({}) "
                "VALUES ({})").format(self.table.render(), columns_part, args_part)

            args = [datarecord.entity_id, datarecord.timestamp] + datarecord.values

            cursor.execute(query, args)

        return f


def prepare_value(value):
    if isinstance(value, dict):
        return str(value)
    else:
        return value
