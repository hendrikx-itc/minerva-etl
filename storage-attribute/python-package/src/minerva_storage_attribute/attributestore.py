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
from itertools import chain
import StringIO

import psycopg2
from minerva.util import head
from minerva.db.query import Table
from minerva.directory.helpers_v4 import get_entitytype_by_id, \
    get_datasource_by_id
from minerva.storage.exceptions import DataError
from minerva.db.error import translate_postgresql_exception, \
    NoSuchColumnError, DataTypeMismatch, UniqueViolation
from minerva.db.dbtransaction import DbTransaction, DbAction, insert_before, \
    replace

from minerva_storage_attribute import schema
from minerva_storage_attribute.attribute import Attribute

MAX_RETRIES = 10

DATATYPE_MISMATCH_ERRORS = set((
    psycopg2.errorcodes.DATATYPE_MISMATCH,
    psycopg2.errorcodes.NUMERIC_VALUE_OUT_OF_RANGE,
    psycopg2.errorcodes.INVALID_TEXT_REPRESENTATION))


class AttributeStore(object):

    """
    Provides the main interface to the attribute storage facilities.

    Use `store` for writing to the attributestore and `retrieve` for reading
    from the attributestore.

    """

    def __init__(self, datasource, entitytype, attributes=tuple()):
        self.id = None
        self.datasource = datasource
        self.entitytype = entitytype

        self.attributes = attributes

        for attr in attributes:
            attr.attributestore = self

        self.table = Table(schema.name, self.table_name())
        self.staging_table = Table(schema.name, self.table_name() + '_staging')
        self.table_curr = Table(schema.name, self.table_name_curr())

    def _insert_query(self, attribute_names):
        """Return a query for inserting a record in the trendstore table."""
        system_columns = "entity_id", "timestamp"
        columns = system_columns + tuple(attribute_names)
        columns_part = ", ".join(map(quote_ident, columns))
        args_part = ", ".join(["%s"] * len(columns))

        return Query(
            "INSERT INTO {0} ({1}) "
            "VALUES ({2})".format(
                self.table.render(), columns_part, args_part))

    def _update_query(self, attribute_names):
        """Return a query for updating a record in the trendstore table."""
        columns_part = ", ".join(map(quote_ident, attribute_names))
        args_part = ", ".join(["%s"] * len(attribute_names))

        return Query(
            "UPDATE {0} SET ({1}) = "
            "({2}) "
            "WHERE entity_id = %s AND timestamp = %s".format(
                self.table.render(), columns_part, args_part))

    def table_name(self):
        """Return the table name for this attributestore."""
        return "{0}_{1}".format(self.datasource.name, self.entitytype.name)

    def table_name_curr(self):
        """Return the table name for this attributestore's current values."""
        return "{0}_curr".format(self.table_name())

    def _update_attributes(self, attributes):
        """Add to, or update current attributes."""
        curr_attributes = list(self.attributes)

        by_name = dict((a.name, a) for a in curr_attributes)

        for a in attributes:
            curr_attribute = by_name.get(a.name)

            if curr_attribute:
                curr_attribute.datatype = a.datatype
            else:
                a.attributestore = self
                curr_attributes.append(a)

        self.attributes = curr_attributes

    def load_attributes(self, cursor):
        """Load associated attributes from database and return them."""
        query = (
            "SELECT id, name, datatype, description "
            "FROM {}.attribute "
            "WHERE attributestore_id = %s").format(schema.name)
        args = self.id,

        cursor.execute(query, args)

        def row_to_attribute(attribute_id, name, datatype, description):
            """Create Attribute, link to this attributestore and return it."""
            attribute = Attribute(name, datatype, description)
            attribute.attributestore = self
            attribute.id = attribute_id
            return attribute

        return map(row_to_attribute, cursor.fetchall())

    @classmethod
    def from_attributes(cls, cursor, datasource, entitytype, attributes):
        """
        Return AttributeStore with specified attributes.

        If an attributestore with specified datasource and entitytype exists,
        it is loaded, or a new one is created if it doesn't.

        """
        query = (
            "SELECT ({}.to_attributestore(%s, %s)).*").format(schema.name)

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
            "FROM {}.attributestore "
            "WHERE datasource_id = %s "
            "AND entitytype_id = %s").format(schema.name)
        args = datasource.id, entitytype.id
        cursor.execute(query, args)

        attributestore_id, = cursor.fetchone()

        attributestore = AttributeStore(datasource, entitytype)
        attributestore.id = attributestore_id
        attributestore.attributes = attributestore.load_attributes(cursor)

        return attributestore

    @classmethod
    def get(cls, cursor, id):
        """Load and return attributestore by its Id."""
        query = (
            "SELECT datasource_id, entitytype_id "
            "FROM attributestore "
            "WHERE id = %s")
        args = id,
        cursor.execute(query, args)

        datasource_id, entitytype_id = cursor.fetchone()

        entitytype = get_entitytype_by_id(cursor, entitytype_id)
        datasource = get_datasource_by_id(cursor, datasource_id)

        attributestore = AttributeStore(datasource, entitytype)
        attributestore.id = id
        attributestore.attributes = attributestore.load_attributes(cursor)

        return attributestore

    def create(self, cursor):
        """Create, initialize and return the attributestore."""
        query = (
            "INSERT INTO {}.attributestore"
            "(datasource_id, entitytype_id) "
            "VALUES (%s, %s) "
            "RETURNING id").format(schema.name)
        args = self.datasource.id, self.entitytype.id
        cursor.execute(query, args)
        self.id = head(cursor.fetchone())

        for attribute in self.attributes:
            attribute.create(cursor)

        return self.init(cursor)

    def init(self, cursor):
        """Create corresponding database table and return self."""
        query = (
            "SELECT {}.init(attributestore) "
            "FROM {}.attributestore "
            "WHERE id = %s").format(schema.name, schema.name)

        args = self.id,

        cursor.execute(query, args)

        return self

    def compact(self, cursor):
        """Combine subsequent records with the same data."""
        query = (
            "SELECT {0.name}.compact(attributestore) "
            "FROM {0.name}.attributestore "
            "WHERE id = %s").format(schema)
        args = self.id,
        cursor.execute(query, args)

    def store(self, datapackage):
        """Return transaction to store the data in the attributestore."""
        return DbTransaction(Insert(self, datapackage))

    def store_batch(self, cursor, datapackage):
        attribute_names = [a.name for a in self.attributes]

        copy_from_query = create_copy_from_query(self.staging_table,
                                                 attribute_names)
        copy_from_file = create_copy_from_file(datapackage)
        cursor.copy_expert(copy_from_query, copy_from_file)

        cursor.execute(
            "SELECT attribute.store_batch(attributestore) "
            "from attribute.attributestore WHERE id = %s", (self.id,))

    def insert_row(self, cursor, column_names, timestamp, entity_id, values):
        if len(values) != len(column_names):
            raise DataError((
                "Number of values does not match number of "
                "attributes ({0} != {1})").format(
                len(values), len(column_names)))

        query = self._insert_query(column_names)

        args = [entity_id, timestamp]
        args.extend(values)

        query.execute(cursor, args)

    def update_row(self, cursor, column_names, timestamp, entity_id, values):
        query = self._update_query(column_names)

        args = list(values)
        args.extend((entity_id, timestamp))

        query.execute(cursor, args)

    def check_attributes_exist(self, cursor):
        query = (
            "SELECT {schema.name}.check_attributes_exist("
            "%s::{attribute}[])").format(
            schema=schema, attribute=schema.attribute.render())

        args = self.attributes,

        cursor.execute(query, args)

    def check_attribute_types(self, cursor):
        query = (
            "SELECT {schema.name}.check_attribute_types("
            "%s::{schema.name}.attribute[])").format(
            schema=schema)

        args = self.attributes,

        cursor.execute(query, args)


class Query(object):
    __slots__ = 'sql',

    def __init__(self, sql):
        self.sql = sql

    def execute(self, cursor, args=None):
        try:
            cursor.execute(self.sql, args)
        except psycopg2.DatabaseError as exc:
            raise translate_postgresql_exception(exc)

        return cursor


def fetch_scalar(cursor):
    return head(cursor.fetchone())


def fetch_one(cursor):
    return cursor.fetch_one()


class Insert(DbAction):
    def __init__(self, attributestore, datapackage):
        self.attributestore = attributestore
        self.datapackage = datapackage

    def execute(self, cursor, state):
        for entity_id, values in self.datapackage.rows:
            try:
                self.attributestore.insert_row(
                    cursor, self.datapackage.attribute_names,
                    self.datapackage.timestamp, entity_id, values)
            except UniqueViolation:
                fix = Update(self.attributestore, self.datapackage)
                return replace(fix)
            except DataTypeMismatch:
                attributes = self.datapackage.deduce_attributes()

                self.attributestore._update_attributes(attributes)

                fix = CheckAttributeTypes(self.attributestore)
                return insert_before(fix)
            except NoSuchColumnError:
                attributes = self.datapackage.deduce_attributes()

                self.attributestore._update_attributes(attributes)

                fix = CheckAttributesExist(self.attributestore)
                return insert_before(fix)


class Update(DbAction):
    def __init__(self, attributestore, datapackage):
        self.attributestore = attributestore
        self.datapackage = datapackage

    def execute(self, cursor, state):
        for entity_id, values in self.datapackage.rows:
            self.attributestore.update_row(cursor,
                                           self.datapackage.attribute_names,
                                           self.datapackage.timestamp,
                                           entity_id, values)


class CheckAttributesExist(DbAction):
    def __init__(self, attributestore):
        self.attributestore = attributestore

    def execute(self, cursor, state):
        self.attributestore.check_attributes_exist(cursor)


class CheckAttributeTypes(DbAction):
    def __init__(self, attributestore):
        self.attributestore = attributestore

    def execute(self, cursor, state):
        self.attributestore.check_attribute_types(cursor)


def create_copy_from_query(table, attribute_names):
    system_columns = "entity_id", "timestamp"
    column_names = chain(system_columns, attribute_names)

    quote = partial(str.format, '"{}"')

    query = "COPY {0}({1}) FROM STDIN".format(
        table.render(), ",".join(map(quote, column_names)))

    return query


def create_copy_from_file(datapackage):
    copy_from_file = StringIO.StringIO()

    lines = create_copy_from_lines(datapackage)

    copy_from_file.writelines(lines)

    copy_from_file.seek(0)

    return copy_from_file


def create_copy_from_lines(datapackage):
    return [create_copy_from_line(datapackage.timestamp, r)
            for r in datapackage.rows]


def create_copy_from_line(timestamp, row):
    entity_id, attributes = row

    values = chain((entity_id, timestamp), attributes)

    return "\t".join(map(str, values)) + "\n"

quote_ident = partial(str.format, '"{}"')
