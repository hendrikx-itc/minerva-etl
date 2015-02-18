# -*- coding: utf-8 -*-
"""Provides AttributeStore class."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from functools import partial

import psycopg2

from minerva.util import head, expand_args, k, no_op
from minerva.db.query import Table
from minerva.directory import EntityType, DataSource
from minerva.db.error import NoSuchColumnError, DataTypeMismatch, \
    translate_postgresql_exception, translate_postgresql_exceptions, \
    UniqueViolation
from minerva.db.dbtransaction import DbTransaction, DbAction, insert_before
from minerva.storage.attribute.attribute import Attribute
from minerva.storage.valuedescriptor import ValueDescriptor
from minerva.storage import datatype

MAX_RETRIES = 10

DATATYPE_MISMATCH_ERRORS = {
    psycopg2.errorcodes.DATATYPE_MISMATCH,
    psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE,
    psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION
}


class NoSuchAttributeError(Exception):
    """Exception type indicating an unknown attribute."""
    pass


class AttributeStoreDescriptor():
    def __init__(self, data_source, entity_type, attribute_descriptors):
        self.data_source = data_source
        self.entity_type = entity_type
        self.attribute_descriptors = attribute_descriptors


class AttributeStore():

    """
    Provides the main interface to the attribute storage facilities.

    Use `store` for writing to the attribute store and `retrieve` for reading
    from the attribute store.

    """

    def __init__(self, id, data_source, entity_type, attributes=tuple()):
        self.id = id
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
    def get_attributes(attribute_store_id):
        """Load associated attributes from database and return them."""
        def f(cursor):
            query = (
                "SELECT id, name, data_type, attributestore_id, description "
                "FROM attribute_directory.attribute "
                "WHERE attributestore_id = %s"
            )
            args = attribute_store_id,

            cursor.execute(query, args)

            def row_to_attribute(row):
                """Create Attribute, link to this attribute store and return it."""
                attribute_id, name, data_type, attribute_store_id, description = row

                return Attribute(
                    attribute_id, name, datatype.type_map[data_type],
                    attribute_store_id, description
                )

            return map(row_to_attribute, cursor.fetchall())

        return f

    @classmethod
    def from_attributes(cls, datasource, entitytype, attribute_descriptors):
        """
        Return AttributeStore with specified attributes.

        If an attribute store with specified datasource and entitytype exists,
        it is loaded, or a new one is created if it doesn't.

        """
        def f(cursor):
            query = (
                "SELECT * "
                "FROM attribute_directory.to_attributestore("
                "%s, %s, %s::attribute_directory.attribute_descr[]"
                ")"
            )

            args = datasource.id, entitytype.id, attribute_descriptors

            cursor.execute(query, args)

            attribute_store_id, _, _ = cursor.fetchone()

            return AttributeStore(
                attribute_store_id,
                datasource,
                entitytype,
                AttributeStore.get_attributes(attribute_store_id)(cursor)
            )

        return f

    @classmethod
    def get_by_attributes(cls, cursor, datasource, entitytype):
        """Load and return AttributeStore with specified attributes."""
        query = (
            "SELECT id "
            "FROM attribute_directory.attributestore "
            "WHERE datasource_id = %s "
            "AND entitytype_id = %s"
        )
        args = datasource.id, entitytype.id
        cursor.execute(query, args)

        attribute_store_id, = cursor.fetchone()

        return AttributeStore(
            attribute_store_id,
            datasource,
            entitytype,
            AttributeStore.get_attributes(attribute_store_id)(cursor)
        )

    @staticmethod
    def get(id):
        """Load and return attribute store by its Id."""
        def f(cursor):
            query = (
                "SELECT datasource_id, entitytype_id "
                "FROM attribute_directory.attributestore "
                "WHERE id = %s"
            )

            args = id,
            cursor.execute(query, args)

            datasource_id, entitytype_id = cursor.fetchone()

            entitytype = EntityType.get(cursor, entitytype_id)
            datasource = DataSource.get(cursor, datasource_id)

            return AttributeStore(
                id, datasource, entitytype,
                AttributeStore.get_attributes(id)(cursor)
            )

        return f

    @staticmethod
    def get_all(cursor):
        """Load and return all attribute stores."""
        query = (
            "SELECT id, datasource_id, entitytype_id "
            "FROM attribute_directory.attributestore"
        )

        cursor.execute(query)

        load = expand_args(partial(AttributeStore.load_attributestore, cursor))

        return map(load, cursor.fetchall())

    @staticmethod
    def load_attributestore(id, datasource_id, entitytype_id):
        def f(cursor):
            data_source = DataSource.get(datasource_id)(cursor)
            entity_type = EntityType.get(entitytype_id)(cursor)

            return AttributeStore(
                id, data_source, entity_type,
                AttributeStore.get_attributes(id)(cursor)
            )

        return f

    @staticmethod
    def create(attribute_store_descriptor):
        """Create, initialize and return the attribute store."""
        def f(cursor):
            query = (
                "SELECT * FROM attribute_directory.create_attributestore("
                "%s, %s, %s::attribute_directory.attribute_descr[]"
                ")"
            )
            args = (
                attribute_store_descriptor.data_source.name,
                attribute_store_descriptor.entity_type.name,
                attribute_store_descriptor.attribute_descriptors
            )
            cursor.execute(query, args)

            attribute_store_id, data_source_id, entity_type_id = cursor.fetchone()

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
            "SELECT attribute_directory.compact(attributestore) "
            "FROM attribute_directory.attributestore "
            "WHERE id = %s"
        )
        args = self.id,
        cursor.execute(query, args)

    def store_txn(self, data_package):
        """Return transaction to store the data in the attribute store."""
        return DbTransaction(
            StoreState(self, data_package),
            [
                StoreBatch()
            ]
        )

    @translate_postgresql_exceptions
    def store_batch(self, cursor, data_package):
        """Write data in one batch using staging table."""
        if data_package.is_empty():
            return

        value_descriptors = self.get_value_descriptors(
            data_package.attribute_names
        )

        data_package.copy_expert(self.staging_table, value_descriptors)(cursor)

        self._transfer_staged(cursor)

    def get_value_descriptors(self, attribute_names):
        """Return list of data types corresponding to the `attribute_names`."""
        attributes_by_name = {a.name: a for a in self.attributes}

        try:
            return [
                ValueDescriptor(
                    name,
                    attributes_by_name[name].data_type,
                    None,
                    {'null_value': '\\N'}
                )
                for name in attribute_names
            ]
        except KeyError:
            raise NoSuchAttributeError()

    def _transfer_staged(self, cursor):
        """Transfer all records from staging to history table."""
        cursor.execute(
            "SELECT attribute_directory.transfer_staged(attributestore) "
            "FROM attribute_directory.attributestore "
            "WHERE id = %s",
            (self.id,)
        )

    def check_attributes_exist(self, attribute_descriptors):
        """Check if attributes exist and create missing."""
        def f(cursor):
            query = (
                "SELECT attribute_directory.check_attributes_exist("
                "attributestore, %s::attribute_directory.attribute_descr[]"
                ") "
                "FROM attribute_directory.attributestore "
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
                "attributestore, "
                "%s::attribute_directory.attribute_descr[]"
                ") "
                "FROM attribute_directory.attributestore WHERE id = %s"
            )

            args = attribute_descriptors, self.id

            cursor.execute(query, args)

            self.attributes = AttributeStore.get_attributes(self.id)(cursor)

        return f

    def store_raw(self, raw_data_package):
        if raw_data_package.is_empty():
            return DbTransaction(None, [])

        return DbTransaction(
            StoreRawState(self, raw_data_package),
            [
                RefineRawDataPackage(),
                StoreBatch()
            ]
        )


class StoreState():
    def __init__(self, attribute_store, data_package):
        self.attribute_store = attribute_store
        self.data_package = data_package


class StoreRawState():
    def __init__(self, attribute_store, raw_data_package):
        self.attribute_store = attribute_store
        self.raw_data_package = raw_data_package
        self.data_package = None


class RefineRawDataPackage(DbAction):
    def execute(self, cursor, state):
        try:
            state.data_package = state.raw_data_package.refine(cursor)
        except UniqueViolation:
            return no_op


class Query():

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


def fetch_scalar(cursor):
    """Return the one scalar result from `cursor`."""
    return head(cursor.fetchone())


def fetch_one(cursor):
    """Return the one record result from `cursor`."""
    return cursor.fetch_one()


class StoreBatch(DbAction):
    """
    A DbAction subclass that calls the 'store_batch' method on the
    attribute store and creates corrective actions if a known error occurs.
    """
    def execute(self, cursor, state):
        try:
            state.attribute_store.store_batch(cursor, state.data_package)
        except psycopg2.DataError as exc:
            if exc.pgcode == psycopg2.errorcodes.BAD_COPY_FILE_FORMAT:
                return insert_before(CheckAttributesExist())
            else:
                raise
        except DataTypeMismatch:
            return insert_before(CheckAttributeTypes())
        except (NoSuchColumnError, NoSuchAttributeError):
            return insert_before(CheckAttributesExist())


class CheckAttributesExist(DbAction):

    """Calls the 'check_attributes_exist' method on the attribute store."""

    def execute(self, cursor, state):
        state.attribute_store.check_attributes_exist(
            state.data_package.deduce_attributes()
        )(cursor)


class CheckAttributeTypes(DbAction):

    """Calls the 'check_attributes_exist' method on the attribute store."""

    def execute(self, cursor, state):
        state.attribute_store.check_attribute_types(
            state.data_package.deduce_attributes()
        )(cursor)
