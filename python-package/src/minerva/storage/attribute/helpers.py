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


class NoSuchAttributeTagError(Exception):
    """
    Exception raised when no matching AttributeTag is found.
    """
    pass


def get_attribute_by_id(conn, attribute_id):
    """
    Return trend with specified id.
    """
    query = (
        "SELECT a.id, a.name, a.description, a.datasource_id, a.entitytype_id "
        "FROM {}.attribute a WHERE a.id = %s ").format(schema.name)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (attribute_id,))

        if cursor.rowcount == 1:
            return Attribute(*cursor.fetchone())
        else:
            raise NoSuchAttributeError("No attribute with id {0}".format(
                attribute_id))


def get_attributetag(conn, name):
    """
    Return attribute tag with specified name.
    """
    with closing(conn.cursor()) as cursor:
        query = (
            "SELECT id, name "
            "FROM {}.tag "
            "WHERE name=%s").format(schema.name)

        cursor.execute(query, (name,))

        if cursor.rowcount == 1:
            id, name = cursor.fetchone()

            return AttributeTag(id, name)
        else:
            raise NoSuchAttributeTagError(
                "No attribute tag with name {0}".format(name))


def create_attributetag(conn, name):
    """
    Create new attribute tag
    :param conn: psycopg2 connection to Minerva database
    :param name: tag name
    """
    with closing(conn.cursor()) as cursor:
        try:
            query = (
                "INSERT INTO {}.tag (id, name) "
                "VALUES (DEFAULT, %s) RETURNING id").format(schema.name)

            cursor.execute(query, (name,))
        except psycopg2.Error as exc:
            conn.rollback()

            if exc.pgcode == psycopg2.errorcodes.UNIQUE_VIOLATION:
                query = (
                    "SELECT id FROM {}.tag"
                    "WHERE name=%s").format(schema.name)

                cursor.execute(query, (name,))

                (id,) = cursor.fetchone()
            else:
                raise exc
        else:
            (id,) = cursor.fetchone()

            conn.commit()

    return AttributeTag(id, name)
