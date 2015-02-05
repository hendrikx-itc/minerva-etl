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
from operator import methodcaller

import psycopg2

from minerva.util import head, expand_args, k, no_op
from minerva.db.query import Table
from minerva.directory import EntityType, DataSource
from minerva.db.error import NoSuchColumnError, DataTypeMismatch, \
    translate_postgresql_exception, translate_postgresql_exceptions, \
    UniqueViolation
from minerva.db.dbtransaction import DbTransaction, DbAction, insert_before
from minerva.storage.attribute.attribute import Attribute

MAX_RETRIES = 10

DATATYPE_MISMATCH_ERRORS = {
    psycopg2.errorcodes.DATATYPE_MISMATCH,
    psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE,
    psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION
}


class NoSuchAttributeError(Exception):
    """Exception type indicating an unknown attribute."""
    pass


class AttributeStore(object):

    """
    Provides the main interface to the attribute storage facilities.

    Use `store` for writing to the attribute store and `retrieve` for reading
    from the attribute store.

    """

    def __init__(self, datasource, entitytype, attributes=tuple()):
        self.id = None
        self.datasource = datasource
        self.entitytype = entitytype

        self.attributes = attributes

        for attr in attributes:
            attr.attributestore = self

        self.table = Table("attribute", self.table_name())
        self.history_table = Table("attribute_history", self.table_name())
        self.staging_table = Table("attribute_staging", self.table_name())
        self.curr_table = Table("attribute", self.table_name())

    def table_name(self):
        """Return the table name for this attribute store."""
        return "{0}_{1}".format(self.datasource.name, self.entitytype.name)

    def update_attributes(self, attributes):
        """Add to, or update current attributes."""
        curr_attributes = list(self.attributes)

        by_name = {a.name: a for a in curr_attributes}

        for attribute in attributes:
            curr_attribute = by_name.get(attribute.name)

            if curr_attribute:
                curr_attribute.datatype = attribute.datatype
            else:
                attribute.attributestore = self
                curr_attributes.append(attribute)

        self.attributes = curr_attributes

    def load_attributes(self, cursor):
        """Load associated attributes from database and return them."""
        query = (
            "SELECT id, name, datatype, description "
            "FROM attribute_directory.attribute "
            "WHERE attributestore_id = %s"
        )
        args = self.id,

        cursor.execute(query, args)

        def row_to_attribute(row):
            """Create Attribute, link to this attribute store and return it."""
            attribute_id, name, datatype, description = row
            attribute = Attribute(name, datatype, description)
            attribute.attributestore = self
            attribute.id = attribute_id
            return attribute

        return map(row_to_attribute, cursor.fetchall())

    @classmethod
    def from_attributes(cls, cursor, datasource, entitytype, attributes):
        """
        Return AttributeStore with specified attributes.

        If an attribute store with specified datasource and entitytype exists,
        it is loaded, or a new one is created if it doesn't.

        """
        query = "SELECT (attribute_directory.to_attributestore(%s, %s)).*"

        args = datasource.id, entitytype.id

        cursor.execute(query, args)

        attributestore_id, _, _ = cursor.fetchone()

        attributestore = AttributeStore(datasource, entitytype, attributes)
        attributestore.id = attributestore_id

        return attributestore

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

        attributestore_id, = cursor.fetchone()

        attributestore = AttributeStore(datasource, entitytype)
        attributestore.id = attributestore_id
        attributestore.attributes = attributestore.load_attributes(cursor)

        return attributestore

    @classmethod
    def get(cls, cursor, id):
        """Load and return attribute store by its Id."""
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

        attributestore = AttributeStore(datasource, entitytype)
        attributestore.id = id
        attributestore.attributes = attributestore.load_attributes(cursor)

        return attributestore

    @classmethod
    def get_all(cls, cursor):
        """Load and return all attribute stores."""
        query = (
            "SELECT id, datasource_id, entitytype_id "
            "FROM attribute_directory.attributestore"
        )

        cursor.execute(query)

        load = expand_args(partial(cls.load_attributestore, cursor))

        return map(load, cursor.fetchall())

    @classmethod
    def load_attributestore(cls, cursor, id, datasource_id, entitytype_id):
        datasource = DataSource.get(cursor, datasource_id)
        entitytype = EntityType.get(cursor, entitytype_id)

        attributestore = AttributeStore(datasource, entitytype)
        attributestore.id = id
        attributestore.attributes = attributestore.load_attributes(cursor)

        return attributestore

    def create(self, cursor):
        """Create, initialize and return the attribute store."""
        query = (
            "INSERT INTO attribute_directory.attributestore"
            "(datasource_id, entitytype_id) "
            "VALUES (%s, %s) "
            "RETURNING id"
        )
        args = self.datasource.id, self.entitytype.id
        cursor.execute(query, args)
        self.id = head(cursor.fetchone())

        for attribute in self.attributes:
            attribute.create(cursor)

        return self.init(cursor)

    def init(self, cursor):
        """Create corresponding database table and return self."""
        query = (
            "SELECT attribute_directory.init(attributestore) "
            "FROM attribute_directory.attributestore "
            "WHERE id = %s"
        )

        args = self.id,

        cursor.execute(query, args)

        return self

    def compact(self, cursor):
        """Combine subsequent records with the same data."""
        query = (
            "SELECT attribute_directory.compact(attributestore) "
            "FROM attribute_directory.attributestore "
            "WHERE id = %s"
        )
        args = self.id,
        cursor.execute(query, args)

    def store_txn(self, datapackage):
        """Return transaction to store the data in the attribute store."""
        return DbTransaction(StoreBatch(self, k(datapackage)))

    @translate_postgresql_exceptions
    def store_batch(self, cursor, datapackage):
        """Write data in one batch using staging table."""
        if datapackage.is_empty():
            return

        data_types = self.get_data_types(datapackage.attribute_names)

        datapackage.copy_expert(self.staging_table, data_types)(cursor)

        self._transfer_staged(cursor)

    def get_data_types(self, attribute_names):
        """Return list of data types corresponding to the `attribute_names`."""
        attributes_by_name = {a.name: a for a in self.attributes}

        try:
            return [
                attributes_by_name[name].datatype
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

    def check_attributes_exist(self, cursor):
        """Check if attributes exist and create missing."""
        query = (
            "SELECT attribute_directory.check_attributes_exist("
            "%s::attribute_directory.attribute[])")

        args = self.attributes,

        cursor.execute(query, args)

    def check_attribute_types(self, cursor):
        """Check and correct attribute data types."""
        query = (
            "SELECT attribute_directory.check_attribute_types("
            "%s::attribute_directory.attribute[])")

        args = self.attributes,

        cursor.execute(query, args)

    def store_raw(self, raw_datapackage):
        if raw_datapackage.is_empty():
            return DbTransaction()

        return DbTransaction(
            RefineRawDataPackage(k(raw_datapackage)),
            StoreBatch(self, read("datapackage"))
        )


class RefineRawDataPackage(DbAction):
    def __init__(self, raw_datapackage):
        self.raw_datapackage = raw_datapackage

    def execute(self, cursor, state):
        raw_datapackage = self.raw_datapackage(state)

        try:
            state["datapackage"] = raw_datapackage.refine(cursor)
        except UniqueViolation:
            return no_op


class Query(object):

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
    def __init__(self, attributestore, datapackage):
        self.attributestore = attributestore
        self.datapackage = datapackage

    def execute(self, cursor, state):
        datapackage = self.datapackage(state)

        try:
            self.attributestore.store_batch(cursor, datapackage)
        except psycopg2.DataError as exc:
            if exc.pgcode == psycopg2.errorcodes.BAD_COPY_FILE_FORMAT:
                attributes = datapackage.deduce_attributes()

                self.attributestore.update_attributes(attributes)

                fix = CheckAttributesExist(self.attributestore)
                return insert_before(fix)
            else:
                raise
        except DataTypeMismatch:
            attributes = datapackage.deduce_attributes()

            self.attributestore.update_attributes(attributes)

            fix = CheckAttributeTypes(self.attributestore)
            return insert_before(fix)
        except (NoSuchColumnError, NoSuchAttributeError) as exc:
            attributes = datapackage.deduce_attributes()

            self.attributestore.update_attributes(attributes)

            fix = CheckAttributesExist(self.attributestore)
            return insert_before(fix)


class CheckAttributesExist(DbAction):

    """Calls the 'check_attributes_exist' method on the attribute store."""

    def __init__(self, attributestore):
        self.attributestore = attributestore

    def execute(self, cursor, state):
        self.attributestore.check_attributes_exist(cursor)


class CheckAttributeTypes(DbAction):

    """Calls the 'check_attributes_exist' method on the attribute store."""

    def __init__(self, attributestore):
        self.attributestore = attributestore

    def execute(self, cursor, state):
        self.attributestore.check_attribute_types(cursor)


read = partial(methodcaller, 'get')
