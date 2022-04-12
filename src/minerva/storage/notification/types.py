# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"
from psycopg2 import sql

from minerva.util import first
from minerva.db.query import Table
from minerva.db.error import translate_postgresql_exceptions


class Record(object):
    """Wraps all data for one notification."""

    def __init__(self, entity_ref, timestamp, attribute_names, values):
        self.entity_ref = entity_ref
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
            "SELECT id "
            "FROM notification.notificationstore "
            "WHERE datasource_id = %s")

        args = datasource.id,

        cursor.execute(query, args)

        if cursor.rowcount == 1:
            notificationstore_id, = cursor.fetchone()

            notificationstore = NotificationStore(datasource, [])
            notificationstore.id = notificationstore_id

            query = (
                "SELECT id, name, data_type, description "
                "FROM notification.attribute "
                "WHERE notificationstore_id = %s"
            )

            args = (notificationstore_id, )

            cursor.execute(query, args)

            for attribute_id, name, data_type, description in cursor.fetchall():
                attribute = Attribute(name, data_type, description)
                attribute.id = attribute_id
                notificationstore.attributes.append(attribute)

            return notificationstore

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

                args = (self.id, attribute.name, attribute.data_type,
                        attribute.description)
                cursor.execute(query, args)

            return self

    def store_record(self, record: Record):
        """Return function that can store the data from a record."""
        @translate_postgresql_exceptions
        def f(cursor):
            column_names = ['entity_id', 'timestamp'] + record.attribute_names
            entity_placeholder, entity_value = record.entity_ref.to_argument()

            placeholders = (
                [entity_placeholder, sql.Placeholder()] +
                (sql.Placeholder() for _ in record.attribute_names)
            )

            query = sql.SQL(
                "INSERT INTO {}({}) VALUES ({})"
            ).format(
                self.table.identifier(),
                sql.SQL(",").join(column_names),
                sql.SQL(",").join(placeholders)
            )

            args = (
                [entity_value, record.timestamp]
                + map(prepare_value, record.values)
            )

            cursor.execute(query, args)

        return f


def prepare_value(value):
    if isinstance(value, dict):
        return str(value)
    else:
        return value
