# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing

from minerva.util import first

from minerva_storage_attribute import schema


def clear_database(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DELETE FROM directory.datasource")
        cursor.execute("DELETE FROM directory.entitytype")
        cursor.execute("DELETE FROM directory.tag")

        system_tables = ["attribute", "attribute_tag_link"]

        all_tables = get_tables(cursor, schema.name)

        attribute_tables = [t for t in all_tables if not t in system_tables]

        for table_name in attribute_tables:
            drop_table(cursor, schema.name, table_name)


def get_tables(cursor, schema):
    query = (
        "SELECT relname FROM pg_class "
        "JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid "
        "WHERE nspname = %s "
        "AND (relkind = 'r' OR relkind = 'v') "
        "AND relhassubclass = true")

    args = (schema, )

    cursor.execute(query, args)

    return map(first, cursor.fetchall())


def drop_table(cursor, schema, table):
    query = 'DROP TABLE "{0}"."{1}" CASCADE'.format(schema, table)

    cursor.execute(query)
