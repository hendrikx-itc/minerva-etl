# -*- coding: utf-8 -*-
"""Provides Attribute class."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from psycopg2.extensions import adapt, register_adapter

from minerva_storage_attribute import schema


class Attribute(object):
    """Describes one attribute of an atttributestore."""
    def __init__(self, name, datatype="smallint", description=None):
        self.id = None
        self.attributestore = None
        self.name = name
        self.description = description
        self.datatype = datatype

    @classmethod
    def get(cls, cursor, id):
        """Load and return attribute by its Id."""
        query = (
            "SELECT name, datatype, description "
            "FROM attribute.attribute "
            "WHERE id = %s")
        args = id,
        cursor.execute(query, args)

        name, datatype, description = cursor.fetchone()

        attribute = Attribute(name, datatype, description)
        attribute.id = id

        return attribute

    def create(self, cursor):
        """Create the attribute in the database."""
        if self.attributestore is None:
            raise Exception("attributestore not set")

        query = (
            "INSERT INTO {0.name}.attribute "
            "(attributestore_id, name, datatype, description) "
            "VALUES (%s, %s, %s, %s)").format(schema)

        args = (self.attributestore.id, self.name, self.datatype,
                self.description)

        cursor.execute(query, args)

    def __repr__(self):
        return "<Attribute({0} {1})>".format(self.name, self.datatype)

    def __str__(self):
        return self.name


def adapt_attribute(attribute):
    """Return psycopg2 compatible representation of `attribute`."""
    if not attribute.attributestore is None:
        attributestore_id = attribute.attributestore.id
    else:
        attributestore_id = None

    attrs = (attribute.id, attributestore_id, attribute.description,
             attribute.name, attribute.datatype)

    return adapt(attrs)


register_adapter(Attribute, adapt_attribute)
