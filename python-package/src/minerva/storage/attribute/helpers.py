# -*- coding: utf-8 -*-
"""
Helper functions for the attribute schema.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing
import psycopg2.errorcodes

from minerva.storage.attribute import schema
from minerva.storage.attribute.attribute import Attribute
from minerva.storage.attribute.basetypes import AttributeTag


class NoSuchAttributeError(Exception):
    """
    Exception raised when no matching AttributeTag is found.
    """
    pass


def get_attribute_by_id(conn, attribute_id):
    """
    Return trend with specified id.
    """
    query = (
        "SELECT a.id, a.name, a.description, astore.datasource_id, "
            "astore.entitytype_id "
        "FROM attribute_directory.attribute a "
        "JOIN attribute_directory.attributestore astore "
            "ON astore.id = a.attributestore_id "
        "WHERE a.id = %s ")

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (attribute_id,))

        if cursor.rowcount == 1:
            return Attribute(*cursor.fetchone())
        else:
            raise NoSuchAttributeError("No attribute with id {0}".format(
                attribute_id))

