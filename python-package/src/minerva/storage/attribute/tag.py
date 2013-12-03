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

from minerva.db.util import create_temp_table, drop_table, \
    create_copy_from_file


def tag_attributes(conn, tag_links):
    """
    Update attribute.attribute_tag_link table

    :param conn: Minerva database connection
    :param tag_links: list of tuples like (attribute_id, tag_name)
    """
    tmp_table_name = store_in_temp_table(conn, tag_links)

    query = (
        "INSERT INTO attribute_directory.attribute_tag_link "
            "(attribute_id, tag_id) "
        "(SELECT tmp.attribute_id, tag.id "
        "FROM {} tmp "
        "JOIN directory.tag tag ON lower(tag.name) = lower(tmp.tag) "
        "LEFT JOIN attribute_directory.attribute_tag_link ttl ON "
        "ttl.attribute_id = tmp.attribute_id AND ttl.tag_id = tag.id "
        "WHERE ttl.attribute_id IS NULL)").format(tmp_table_name)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

    drop_table(conn, tmp_table_name)

    conn.commit()


def store_in_temp_table(conn, tag_links):
    """
    Create temporay table with tag links

    :param conn: Minerva database connection
    :param tag_links: list of tuples like (attribute_id, tag_name)
    """

    tmp_table_name = "tmp_attribute_tags"
    columns = [
        ("attribute_id", "integer"),
        ("tag", "varchar")
    ]
    column_names = [col_name for col_name, col_type in columns]
    sql_columns = ["{} {}".format(*column) for column in columns]

    copy_from_file = create_copy_from_file(tag_links, ('d', 's'))
    create_temp_table(conn, tmp_table_name, sql_columns)

    with closing(conn.cursor()) as cursor:
        cursor.copy_from(copy_from_file, tmp_table_name, columns=column_names)

    return tmp_table_name


def flush_tag_links(conn, tag_name):
    """
    Remove tag links for a specific tag
    :param conn: Minerva database connection
    :param tag_name: tag specifying attribute tags links that will be removed
    """
    query = (
        "DELETE FROM attribute_directory.attribute_tag_link atl "
        "USING directory.tag tag "
        "WHERE tag.id = atl.tag_id AND lower(tag.name) = lower(%s)")

    args = (tag_name,)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, args)
