# -*- coding: utf-8 -*-
"""Provides AttributeStore class."""
from contextlib import closing
from functools import partial

import psycopg2

from minerva.util import expand_args
from minerva.db.query import Table
from minerva.directory import EntityType, DataSource
from minerva.db.error import translate_postgresql_exception, \
    translate_postgresql_exceptions
from minerva.storage.attribute.attribute import Attribute
from minerva.storage.outputdescriptor import OutputDescriptor
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage import datatype


class NoSuchAttributeError(Exception):
    """Exception type indicating an unknown attribute."""
    pass


class AttributeStoreDescriptor:
    def __init__(self, data_source, entity_type, attribute_descriptors):
        self.data_source = data_source
        self.entity_type = entity_type
        self.attribute_descriptors = attribute_descriptors


class AttributeStore:

    """
    Provides the main interface to the attribute storage facilities.

    Use `store` for writing to the attribute store and `retrieve` for reading
    from the attribute store.

    """

    def __init__(self, id_, data_source, entity_type, attributes):
        self.id = id_
        self.data_source = data_source
        self.entity_type = entity_type

        self.attributes = attributes

        for attr in attributes:
            attr.attribute_store = self

        self.table = Table("attribute", self.table_name())
        self.history_table = Table("attribute_history", self.table_name())
        self.staging_table = Table("attribute_staging", self.table_name())
        self.curr_table = Table("attribute", self.table_name())

    def table_name(self):
        """Return the table name for this attribute store."""
        return "{0}_{1}".format(self.data_source.name, self.entity_type.name)

    def update_attributes(self, attribute_descriptors):
        """Add to, or update current attributes."""
        def f(cursor):
            self.check_attributes_exist(attribute_descriptors)(cursor)
            self.check_attribute_types(attribute_descriptors)(cursor)

        return f

    @staticmethod
    def get_attributes(attribute_store_id: int):
        """Load associated attributes from database and return them.
        :param attribute_store_id: Unique database ID of the attribute store
        """
        def f(cursor):
            query = (
                "SELECT id, name, data_type, attribute_store_id, description "
                "FROM attribute_directory.attribute "
                "WHERE attribute_store_id = %s"
            )
            args = attribute_store_id,

            cursor.execute(query, args)

            def row_to_attribute(row):
                (
                    attribute_id, name, data_type, attribute_store_id_,
                    description
                ) = row

                return Attribute(
                    attribute_id, name, datatype.registry[data_type],
                    attribute_store_id, description
                )

            return [row_to_attribute(row) for row in cursor.fetchall()]

        return f

    @classmethod
    def from_attributes(cls, data_source, entity_type, attribute_descriptors):
        """
        Return AttributeStore with specified attributes.

        If an attribute store with specified data source and entity type
        exists, it is loaded, or a new one is created if it doesn't.

        """
        def f(cursor):
            query = (
                "SELECT * "
                "FROM attribute_directory.to_attribute_store("
                "%s, %s, %s::attribute_directory.attribute_descr[]"
                ")"
            )

            args = data_source.id, entity_type.id, attribute_descriptors

            cursor.execute(query, args)

            attribute_store_id, _, _ = cursor.fetchone()

            return AttributeStore(
                attribute_store_id,
                data_source,
                entity_type,
                AttributeStore.get_attributes(attribute_store_id)(cursor)
            )

        return f

    @classmethod
    def get_by_attributes(cls, data_source, entity_type):
        """Load and return AttributeStore with specified attributes."""
        def f(cursor):
            query = (
                "SELECT id "
                "FROM attribute_directory.attribute_store "
                "WHERE data_source_id = %s "
                "AND entity_type_id = %s"
            )
            args = data_source.id, entity_type.id
            cursor.execute(query, args)

            attribute_store_id, = cursor.fetchone()

            return AttributeStore(
                attribute_store_id,
                data_source,
                entity_type,
                AttributeStore.get_attributes(attribute_store_id)(cursor)
            )

        return f

    @staticmethod
    def get(id_: int):
        """
        Load and return attribute store by its Id.

        :param id_: Unique database ID of the attribute store
        """
        def f(cursor):
            query = (
                "SELECT data_source_id, entity_type_id "
                "FROM attribute_directory.attribute_store "
                "WHERE id = %s"
            )

            args = id_,
            cursor.execute(query, args)

            data_source_id, entity_type_id = cursor.fetchone()

            entity_type = EntityType.get(entity_type_id)(cursor)
            data_source = DataSource.get(data_source_id)(cursor)

            return AttributeStore(
                id_, data_source, entity_type,
                AttributeStore.get_attributes(id_)(cursor)
            )

        return f

    @staticmethod
    def get_all(cursor):
        """Load and return all attribute stores."""
        query = (
            "SELECT id, data_source_id, entity_type_id "
            "FROM attribute_directory.attribute_store"
        )

        cursor.execute(query)

        load = expand_args(partial(
            AttributeStore.load_attribute_store, cursor))

        return map(load, cursor.fetchall())

    @staticmethod
    def load_attribute_store(
            id_: int, data_source_id: int, entity_type_id: int):
        def f(cursor):
            data_source = DataSource.get(data_source_id)(cursor)
            entity_type = EntityType.get(entity_type_id)(cursor)

            return AttributeStore(
                id_, data_source, entity_type,
                AttributeStore.get_attributes(id_)(cursor)
            )

        return f

    @staticmethod
    def create(attribute_store_descriptor):
        """Create, initialize and return the attribute store."""
        def f(cursor):
            query = (
                "SELECT * FROM attribute_directory.create_attribute_store("
                "%s, %s, %s::attribute_directory.attribute_descr[]"
                ")"
            )

            args = (
                attribute_store_descriptor.data_source.name,
                attribute_store_descriptor.entity_type.name,
                attribute_store_descriptor.attribute_descriptors
            )
            cursor.execute(query, args)

            (
                attribute_store_id, data_source_id, entity_type_id
            ) = cursor.fetchone()

            entity_type = EntityType.get(entity_type_id)(cursor)
            data_source = DataSource.get(data_source_id)(cursor)

            attributes = AttributeStore.get_attributes(
                attribute_store_id
            )(cursor)

            return AttributeStore(
                attribute_store_id, data_source, entity_type, attributes
            )

        return f

    def compact(self, cursor):
        """Combine subsequent records with the same data."""
        query = (
            "SELECT attribute_directory.compact(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s"
        )
        args = self.id,
        cursor.execute(query, args)

    def _stage_data(self, cursor, data_package):
        output_descriptors = self.get_output_descriptors(
            data_package.attribute_names
        )

        data_package.copy_expert(
            self.staging_table, output_descriptors
        )(cursor)

    @translate_postgresql_exceptions
    def store(self, data_package):
        """Write data in one batch using staging table."""
        def f(conn):
            if data_package.is_empty():
                return

            with closing(conn.cursor()) as cursor:
                self._stage_data(cursor, data_package)
                self._transfer_staged(cursor)

        return f

    def get_value_descriptors(self, attribute_names):
        """Return list of data types corresponding to the `attribute_names`."""
        attributes_by_name = {a.name: a for a in self.attributes}

        def name_to_value_descriptor(name):
            try:
                return ValueDescriptor(
                    name,
                    attributes_by_name[name].data_type
                )
            except KeyError:
                raise NoSuchAttributeError(name)

        return [name_to_value_descriptor(name) for name in attribute_names]

    def get_output_descriptors(self, attribute_names):
        """Return list of data types corresponding to the `attribute_names`."""
        return [
            OutputDescriptor(value_descriptor, None)
            for value_descriptor in self.get_value_descriptors(attribute_names)
        ]

    def _transfer_staged(self, cursor):
        """Transfer all records from staging to history table."""
        cursor.execute(
            "SELECT attribute_directory.transfer_staged(attribute_store) "
            "FROM attribute_directory.attribute_store "
            "WHERE id = %s",
            (self.id,)
        )

    def check_attributes_exist(self, attribute_descriptors):
        """Check if attributes exist and create missing."""
        def f(cursor):
            query = (
                "SELECT attribute_directory.check_attributes_exist("
                "attribute_store, %s::attribute_directory.attribute_descr[]"
                ") "
                "FROM attribute_directory.attribute_store "
                "WHERE id = %s"
            )

            args = attribute_descriptors, self.id

            cursor.execute(query, args)

            self.attributes = AttributeStore.get_attributes(self.id)(cursor)

        return f

    def check_attribute_types(self, attribute_descriptors):
        """Check and correct attribute data types."""
        def f(cursor):
            query = (
                "SELECT attribute_directory.check_attribute_types("
                "attribute_store, "
                "%s::attribute_directory.attribute_descr[]"
                ") "
                "FROM attribute_directory.attribute_store WHERE id = %s"
            )

            args = attribute_descriptors, self.id

            cursor.execute(query, args)

            self.attributes = AttributeStore.get_attributes(self.id)(cursor)

        return f


class Query:

    """Generic query wrapper."""

    __slots__ = 'sql',

    def __init__(self, sql):
        self.sql = sql

    def execute(self, cursor, args=None):
        """Execute wrapped query with provided cursor and arguments."""
        try:
            cursor.execute(self.sql, args)
        except psycopg2.DatabaseError as exc:
            raise translate_postgresql_exception(exc)

        return cursor
