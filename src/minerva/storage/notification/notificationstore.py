# -*- coding: utf-8 -*-
from minerva.db.query import Table, smart_quote
from minerva.db.error import translate_postgresql_exceptions
from minerva.directory import DataSource
from minerva.storage.notification.attribute import Attribute


class NotificationStoreDescriptor:
    def __init__(self, data_source, attribute_descriptors):
        self.data_source = data_source
        self.attribute_descriptors = attribute_descriptors


class NotificationStore:
    def __init__(self, id_, data_source, attributes):
        self.id = id_
        self.data_source = data_source
        self.attributes = attributes
        self.table = Table("notification", data_source.name)

    @staticmethod
    def load(cursor, data_source):
        """Load NotificationStore from database and return it."""
        query = (
            "SELECT id "
            "FROM notification_directory.notification_store "
            "WHERE data_source_id = %s")

        args = data_source.id,

        cursor.execute(query, args)

        if cursor.rowcount == 1:
            notification_store_id, = cursor.fetchone()

            return NotificationStore(
                notification_store_id, data_source,
                NotificationStore.get_attributes(notification_store_id)(cursor)
            )

    @staticmethod
    def get_attributes(notification_store_id):
        def f(cursor):
            query = (
                "SELECT id, notification_store_id, name, data_type, "
                "description "
                "FROM notification_directory.attribute "
                "WHERE notification_store_id = %s"
            )

            args = (notification_store_id, )

            cursor.execute(query, args)

            return [
                Attribute(attribute_id, ns_id, name, data_type, description)
                for attribute_id, ns_id, name, data_type, description
                in cursor.fetchall()
            ]

        return f

    @staticmethod
    def create(notification_store_descriptor):
        """Create notification store in database"""
        def f(cursor):
            query = (
                "SELECT * "
                "FROM notification_directory.create_notification_store("
                "%s, %s::notification_directory.attr_def[]"
                ")"
            )

            args = (
                notification_store_descriptor.data_source.id,
                notification_store_descriptor.attribute_descriptors
            )

            cursor.execute(query, args)

            notification_store_id, data_source_id = cursor.fetchone()

            data_source = DataSource.get(data_source_id)(cursor)

            attributes = NotificationStore.get_attributes(
                notification_store_id
            )(cursor)

            return NotificationStore(
                notification_store_id, data_source, attributes
            )

        return f

    def store_record(self, record):
        """Return function that can store the data from a
        :class:`~minerva.storage.notification.types.Record`."""

        @translate_postgresql_exceptions
        def f(cursor):
            column_names = ['entity_id', 'timestamp'] + record.attribute_names
            columns_part = ','.join(map(smart_quote, column_names))

            entity_placeholder, entity_value = record.entity_ref.to_argument()

            placeholders = (
                [entity_placeholder, "%s"] +
                (["%s"] * len(record.attribute_names))
            )

            query = (
                "INSERT INTO {} ({}) "
                "VALUES ({})"
            ).format(self.table.render(), columns_part, ",".join(placeholders))

            args = (
                [entity_value, record.timestamp]
                + list(map(prepare_value, record.values))
            )

            cursor.execute(query, args)

        return f


def prepare_value(value):
    if isinstance(value, dict):
        return str(value)
    else:
        return value
